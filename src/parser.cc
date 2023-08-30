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
        case TokenType::StringLiteral:
        case TokenType::TrueLiteral:
        case TokenType::FalseLiteral:
            return new Literal(NextToken());
        case TokenType::Identifier:
            return new ColRef(NextToken());
        case TokenType::LParen: {
            NextToken(); //(
            Expr* expr = ParseExpr();
            NextToken(); //)
            return expr;
        }
        default:
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

Expr* Parser::ParseAssignment() {
    Token t = NextToken();
    NextToken(); //=
    Expr* right = ParseExpr();

    return new Assignment(t, right);    
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

            std::vector<Token> attrs;
            std::vector<Token> types;
            while (PeekToken().type != TokenType::RParen) {
                attrs.push_back(NextToken());
                types.push_back(NextToken());
                if (PeekToken().type == TokenType::Primary) {
                    NextToken(); //primary
                    NextToken(); //key
                }

                if (PeekToken().type == TokenType::Comma) {
                    NextToken();//,
                }
            }

            NextToken();//)
            NextToken();//;
            return new CreateStmt(target, attrs, types);
        }
        case TokenType::Insert: {
            NextToken(); //into
            Token target = NextToken(); //table name
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
            std::vector<Expr*> target_cols;
            while (PeekToken().type != TokenType::From) {
                target_cols.push_back(ParseExpr());
                if (PeekToken().type == TokenType::Comma) {
                    NextToken(); //,
                }
            }
            NextToken(); //from
            Token target = NextToken();
            Expr* where_clause = nullptr;
            if (PeekToken().type == TokenType::Where) {
                NextToken(); //where
                where_clause = ParseExpr(); 
            } else {
                where_clause = new Literal(true);
            }
            NextToken(); //;
            return new SelectStmt(target, target_cols, where_clause);
        }

        case TokenType::Update: {
            Token target = NextToken();
            NextToken(); //set
            std::vector<Expr*> assignments;
            while (!(PeekToken().type == TokenType::SemiColon || PeekToken().type == TokenType::Where)) {
                assignments.push_back(ParseAssignment());
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

            return new UpdateStmt(target, assignments, where_clause);
        }
        case TokenType::Delete: {
            NextToken(); //from
            Token target = NextToken();

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
    }

    return NULL;
}

}
