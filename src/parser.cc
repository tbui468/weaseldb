#include <vector>

#include "parser.h"

namespace wsldb {


std::vector<Stmt*> Parser::ParseStmts() {
    std::vector<Stmt*> stmts = std::vector<Stmt*>();

    while (PeekToken().type != TokenType::Eof) {
        stmts.push_back(ParseStmt());    
    }
    
    return stmts;
}

Expr* Parser::ParsePrimary() {
    switch (PeekToken().type) {
        case TokenType::IntLiteral:
        case TokenType::FloatLiteral:
        case TokenType::StringLiteral:
        case TokenType::TrueLiteral:
        case TokenType::FalseLiteral:
            return new Literal(NextToken());
        case TokenType::Identifier: {
            Token ref = NextToken();
            if (PeekToken().type == TokenType::Dot) {
                NextToken(); //.
                Token col = NextToken();
                return new ColRef(col, ref);
            }
            return new ColRef(ref);
        }
        case TokenType::LParen: {
            NextToken(); //(
            Expr* expr = ParseExpr();
            NextToken(); //)
            return expr;
        }
        default:
            if (TokenTypeIsAggregateFunction(PeekToken().type)) {
                Token fcn = NextToken();
                NextToken(); //(
                Expr* arg = ParseExpr();
                NextToken(); //)
                return new Call(fcn, arg);
            }
            return NULL;
    }
    return NULL;
}

Expr* Parser::ParseUnary() {
    if (PeekToken().type == TokenType::Minus ||
           PeekToken().type == TokenType::Not) {
        Token op = NextToken();
        return new Unary(op, ParseUnary());
    }

    return ParsePrimary();
}

Expr* Parser::ParseMultiplicative() {
    Expr* left = ParseUnary();

    while (PeekToken().type == TokenType::Star ||
           PeekToken().type == TokenType::Slash) {
        Token op = NextToken();
        left = new Binary(op, left, ParseUnary());
    }

    return left;
}

Expr* Parser::ParseAdditive() {
    Expr* left = ParseMultiplicative();

    while (PeekToken().type == TokenType::Plus ||
           PeekToken().type == TokenType::Minus) {
        Token op = NextToken();
        left = new Binary(op, left, ParseMultiplicative());
    }

    return left;
}

Expr* Parser::ParseRelational() {
    Expr* left = ParseAdditive();

    while (PeekToken().type == TokenType::Less ||
           PeekToken().type == TokenType::LessEqual ||
           PeekToken().type == TokenType::Greater ||
           PeekToken().type == TokenType::GreaterEqual) {
        Token op = NextToken();
        left = new Binary(op, left, ParseAdditive());
    }

    return left;
}

Expr* Parser::ParseEquality() {
    Expr* left = ParseRelational();

    while (PeekToken().type == TokenType::Equal ||
           PeekToken().type == TokenType::NotEqual) {
        Token op = NextToken();
        left = new Binary(op, left, ParseRelational());
    }

    return left;
}

Expr* Parser::ParseAnd() {
    Expr* left = ParseEquality();

    while (PeekToken().type == TokenType::And) {
        Token op = NextToken();
        left = new Binary(op, left, ParseEquality());
    }

    return left;
}

Expr* Parser::ParseOr() {
    Expr* left = ParseAnd();

    while (PeekToken().type == TokenType::Or) {
        Token op = NextToken();
        left = new Binary(op, left, ParseAnd());
    }

    return left;
}

Expr* Parser::ParseExpr() {
    return ParseOr();
}

WorkTable* Parser::ParsePrimaryWorkTable() {
    Token t = NextToken();
    if (PeekToken().type == TokenType::As) {
        NextToken(); //as
        Token alias = NextToken();
        return new Physical(t, alias);
    }

    return new Physical(t);
}

WorkTable* Parser::ParseBinaryWorkTable() {
    WorkTable* left = ParsePrimaryWorkTable();
    while (PeekToken().type == TokenType::Cross) {
        NextToken(); //cross
        NextToken(); //join
        left = new CrossJoin(left, ParsePrimaryWorkTable());
    }

    return left;
}

WorkTable* Parser::ParseWorkTable() {
    return ParseBinaryWorkTable();
}

std::vector<Expr*> Parser::ParseTuple() {
    NextToken(); //(
    std::vector<Expr*> exprs = std::vector<Expr*>();
    while (PeekToken().type != TokenType::RParen) {
        exprs.push_back(ParseExpr());
        if (PeekToken().type == TokenType::Comma) {
            NextToken(); //,
        }
    }
    NextToken(); //)
    return exprs;
}

Stmt* Parser::ParseStmt() {
    switch (NextToken().type) {
        case TokenType::Create: {
            NextToken(); //table
            Token target = NextToken();
            NextToken(); //(

            std::vector<Token> names;
            std::vector<Token> types;
            std::vector<Token> pks;
            while (PeekToken().type != TokenType::RParen) {
                if (PeekToken().type == TokenType::Primary) {
                    NextToken(); //primary
                    NextToken(); //key
                    NextToken(); //(
                    while (PeekToken().type != TokenType::RParen) {
                        pks.push_back(NextToken());
                        if (PeekToken().type == TokenType::Comma) {
                            NextToken();
                        } 
                    }
                    NextToken(); //)
                    /*
                } else if (PeekToken().type == TokenType::Foreign) {
                    NextToken(); //foreign
                    NextToken(); //key
                    std::vector<Expr*> cols = ParseTuple();
                    NextToken(); //references
                    Token foreign_target = NextToken();
                    std::vector<Expr*> foreign_cols = ParseTuple();
                    constraints.push_back(new ForeignKey(cols, foreign_target, foreign_cols));*/
                } else {
                    names.push_back(NextToken());
                    types.push_back(NextToken());
                }

                if (PeekToken().type == TokenType::Comma) {
                    NextToken();//,
                }
            }

            NextToken();//)
            NextToken();//;
            return new CreateStmt(target, names, types, pks);
        }
        case TokenType::Insert: {
            NextToken(); //into
            WorkTable* target = ParseWorkTable(); //table name
            NextToken(); //values
            std::vector<std::vector<Expr*>> values = std::vector<std::vector<Expr*>>();
            while (PeekToken().type == TokenType::LParen) {
                values.push_back(ParseTuple());
                if (PeekToken().type == TokenType::Comma) {
                    NextToken(); //,
                }
            }
            NextToken(); //;
            return new InsertStmt(target, values);
        }
        case TokenType::Select: {
            bool remove_duplicates = false;
            if (PeekToken().type == TokenType::Distinct) {
                NextToken(); //distinct
                remove_duplicates = true;
            }

            std::vector<Expr*> target_cols;
            target_cols.push_back(ParseExpr());
            while (PeekToken().type == TokenType::Comma) {
                NextToken(); //,
                target_cols.push_back(ParseExpr());
            }

            WorkTable* target = nullptr;
            if (PeekToken().type == TokenType::From) {
                NextToken(); //from
                target = ParseWorkTable();
            } else {
                target = new ConstantTable(target_cols.size());
            }

            Expr* where_clause = nullptr;
            if (PeekToken().type == TokenType::Where) {
                NextToken(); //where
                where_clause = ParseExpr(); 
            } else {
                where_clause = new Literal(true);
            }

            std::vector<OrderCol> order_cols;
            if (PeekToken().type == TokenType::Order) {
                NextToken(); //order
                NextToken(); //by

                Expr* col = ParseExpr();
                Token asc = NextToken();
                order_cols.push_back({col, asc.type == TokenType::Asc ? new Literal(true) : new Literal(false)});

                while (PeekToken().type == TokenType::Comma) {
                    NextToken();
                    Expr* col = ParseExpr();
                    Token asc = NextToken();
                    order_cols.push_back({col, asc.type == TokenType::Asc ? new Literal(true) : new Literal(false)});
                }
            }

            Expr* limit = nullptr;
            if (PeekToken().type == TokenType::Limit) {
                NextToken(); //limit
                limit = ParseExpr();
            } else {
                limit = new Literal(-1); //no limit
            } 

            NextToken(); //;
            return new SelectStmt(target, target_cols, where_clause, order_cols, limit, remove_duplicates);
        }

        case TokenType::Update: {
            WorkTable* target = ParseWorkTable();
            NextToken(); //set

            std::vector<Expr*> assigns;
            while (!(PeekToken().type == TokenType::SemiColon || PeekToken().type == TokenType::Where)) {
                Token col = NextToken();
                NextToken();
                assigns.push_back(new ColAssign(col, ParseExpr()));
                if (PeekToken().type == TokenType::Comma) {
                    NextToken(); //,
                }
            }

            Expr* where_clause = nullptr;
            if (PeekToken().type == TokenType::Where) {
                NextToken(); //where
                where_clause = ParseExpr(); 
            } else {
                where_clause = new Literal(true);
            }
            NextToken(); //;

            return new UpdateStmt(target, assigns, where_clause);
        }
        case TokenType::Delete: {
            NextToken(); //from
            WorkTable* target = ParseWorkTable();

            Expr* where_clause = nullptr;
            if (PeekToken().type == TokenType::Where) {
                NextToken(); //where
                where_clause = ParseExpr(); 
            } else {
                where_clause = new Literal(true);
            }
            NextToken(); //;
            return new DeleteStmt(target, where_clause);
        }
        case TokenType::Drop: {
            NextToken(); //table
            bool has_if_exists = false;
            if (PeekToken().type == TokenType::If) {
                has_if_exists = true;
                NextToken(); //if
                NextToken(); //exists
            }
            Token target = NextToken();
            NextToken(); //;
            return new DropTableStmt(target, has_if_exists);
        }
        case TokenType::Describe: {
            NextToken(); //table
            Token target = NextToken();
            NextToken(); //;
            return new DescribeTableStmt(target);
        }
    }

    return NULL;
}

}
