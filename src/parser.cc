#include <vector>
#include <algorithm>

#include "parser.h"

namespace wsldb {

#define ParseExpr(fcn) \
    ({ Expr* expr_; \
       Status s = fcn(&expr_); \
       if (!s.Ok()) return s; \
       expr_; \
    })

#define ParseScan(fcn) \
    ({ Scan* scan_; \
       Status s = fcn(&scan_); \
       if (!s.Ok()) return s; \
       scan_; \
    })

#define EatToken(expected_type, err_msg) \
    ({ Token t_ = NextToken(); \
       if (t_.type != expected_type) return Status(false, err_msg); \
       t_; })

#define EatTokenIn(tv, err_msg) \
    ({ Token t_ = NextToken(); \
       if (std::find(tv.begin(), tv.end(), t_.type) == tv.end()) return Status(false, err_msg); \
       t_; })

Status Parser::ParseStmts(std::vector<Stmt*>& stmts) {
    while (!AdvanceIf(TokenType::Eof)) {
        Stmt* stmt;
        Status s = ParseStmt(&stmt);
        if (!s.Ok())
            return s;

        stmts.push_back(stmt);
    }

    return Status();
}

Column Parser::ParseColumn() {
    Token ref = NextToken(); //either column name or table
    if (AdvanceIf(TokenType::Dot)) {
        Token col = NextToken();
        return { ref.lexeme, col.lexeme };
    }

    return { "", ref.lexeme };
}

Status Parser::Primary(Expr** expr) {
    switch (PeekToken().type) {
        case TokenType::IntLiteral:
        case TokenType::FloatLiteral:
        case TokenType::StringLiteral:
        case TokenType::TrueLiteral:
        case TokenType::FalseLiteral:
        case TokenType::Null:
        case TokenType::ByteaLiteral:
        case TokenType::Star: //column wildcard
            *expr = new Literal(NextToken());
            return Status();
        case TokenType::Cast: {
            NextToken();
            EatToken(TokenType::LParen, "Parse Error: Expected '(' after 'cast'");
            Expr* value = ParseExpr(Base);
            EatToken(TokenType::As, "Parse Error: Expected keyword 'as' after expression to cast");
            Token type = EatTokenIn(TokenTypeSQLDataTypes(), "Parse Error: Expected valid SQL data type");
            EatToken(TokenType::RParen, "Parse Error: Expected ')' after cast type"); 
            *expr = new Cast(value, type);
            return Status();
        }
        case TokenType::Identifier: {
            Token ref = NextToken();
            if (PeekToken().type == TokenType::LParen) { //custom function call (possibly ML model)
                NextToken();
                Expr* arg = ParseExpr(Base);
                EatToken(TokenType::RParen, "Parse Error: Expected ')' after function argument"); 
                *expr = new Predict(ref, arg);
                return Status();
            } else { //column reference
                if (AdvanceIf(TokenType::Dot)) {
                    Token name = NextToken();
                    *expr = new ColRef({ref.lexeme, name.lexeme});
                    return Status();
                }
                *expr = new ColRef({"", ref.lexeme});
                return Status();
            }
        }
        case TokenType::LParen: {
            NextToken(); //(
            *expr = ParseExpr(Base);
            EatToken(TokenType::RParen, "Parse Error: Expected ')' after expression");
            return Status();
        }
        default:
            if (TokenTypeIsAggregateFunction(PeekToken().type)) {
                Token fcn = NextToken();
                EatToken(TokenType::LParen, "Parse Error: Expected '(' after function name");
                Expr* arg = ParseExpr(Base);
                EatToken(TokenType::RParen, "Parse Error: Expected ')' after expression");
                *expr = new Call(fcn, arg);
                return Status();
            }
            return Status(false, "Parse Error: Invalid function name");
    }
    return Status(false, "Parse Error: Invalid token");
}

Status Parser::ParseUnary(Expr** expr) {
    if (PeekToken().type == TokenType::Minus ||
           PeekToken().type == TokenType::Not) {
        Token op = NextToken();
        Expr* right = ParseExpr(ParseUnary);
        *expr = new Unary(op, right);
        return Status();
    }

    *expr = ParseExpr(Primary);
    return Status();
}

Status Parser::Multiplicative(Expr** expr) {
    Expr* left = ParseExpr(ParseUnary);

    while (PeekToken().type == TokenType::Star ||
           PeekToken().type == TokenType::Slash) {
        Token op = NextToken();
        Expr* right = ParseExpr(ParseUnary);
        left = new Binary(op, left, right);
    }

    *expr = left;
    return Status();
}

Status Parser::Additive(Expr** expr) {
    Expr* left = ParseExpr(Multiplicative);

    while (PeekToken().type == TokenType::Plus ||
           PeekToken().type == TokenType::Minus) {
        Token op = NextToken();
        Expr* right = ParseExpr(Multiplicative);
        left = new Binary(op, left, right);
    }

    *expr = left;
    return Status();
}

Status Parser::Relational(Expr** expr) {
    Expr* left = ParseExpr(Additive);

    while (PeekToken().type == TokenType::Less ||
           PeekToken().type == TokenType::LessEqual ||
           PeekToken().type == TokenType::Greater ||
           PeekToken().type == TokenType::GreaterEqual) {
        Token op = NextToken();
        Expr* right = ParseExpr(Additive);
        left = new Binary(op, left, right);
    }

    *expr = left;
    return Status();
}

Status Parser::Equality(Expr** expr) {
    Expr* left = ParseExpr(Relational);

    while (PeekToken().type == TokenType::Equal ||
           PeekToken().type == TokenType::NotEqual ||
           PeekToken().type == TokenType::Is ||
           PeekToken().type == TokenType::Like ||
           PeekToken().type == TokenType::Similar ||
           (PeekToken().type == TokenType::Not && (PeekTwo().type == TokenType::Like || PeekTwo().type == TokenType::Similar))) {

        Token op = NextToken();
        if (op.type == TokenType::Is) {
            Token t = EatTokenIn(std::vector<TokenType>({TokenType::Null, TokenType::Not}), 
                                 "Parse Error: Keyword 'is' must be followed by 'null' or 'not null'");
            if (t.type == TokenType::Null) {
                left = new IsNull(left);
            } else { //t.type must be 'not'
                EatToken(TokenType::Null, "Parse Error: Keyword 'is' must be followed by 'null' or 'not null'");
                left = new Unary(t, new IsNull(left));
            } 
        } else if (op.type == TokenType::Not) {
            Token like_or_similar = EatTokenIn(std::vector<TokenType>({TokenType::Like, TokenType::Similar}), 
                                               "Parse Error: Expected 'like' or 'similar' keyword after 'not'");
            if (like_or_similar.type == TokenType::Similar) {
                EatToken(TokenType::To, "Parse Error: Expect keyword 'to' after 'similar'");
            }
            Expr* right = ParseExpr(Relational);
            left = new Unary(op, new Binary(like_or_similar, left, right));
        } else {
            if (op.type == TokenType::Similar) {
                EatToken(TokenType::To, "Parse Error: Expect keyword 'to' after 'similar'");
            }
            Expr* right = ParseExpr(Relational);
            left = new Binary(op, left, right);
        }
    }

    *expr = left;
    return Status();
}

Status Parser::And(Expr** expr) {
    Expr* left = ParseExpr(Equality);

    while (PeekToken().type == TokenType::And) {
        Token op = NextToken();
        Expr* right = ParseExpr(Equality);
        left = new Binary(op, left, right);
    }

    *expr = left;
    return Status();
}

Status Parser::Or(Expr** expr) {
    Expr* left = ParseExpr(And);

    while (PeekToken().type == TokenType::Or) {
        Token op = NextToken();
        Expr* right = ParseExpr(And);
        left = new Binary(op, left, right);
    }

    *expr = left;
    return Status();
}

Status Parser::Base(Expr** expr) {
    if (PeekToken().type == TokenType::Select) {
        Stmt* stmt;
        Status s = ParseStmt(&stmt);
        if (!s.Ok()) return s;
        *expr = new ScalarSubquery(stmt);
        return Status();
    }

    *expr = ParseExpr(Or);
    return Status();
}

Status Parser::ParsePrimaryScan(Scan** wt) {
    Token t = NextToken();
    if (AdvanceIf(TokenType::As)) {
        Token alias = NextToken();
        *wt = new TableScan(t, alias);
        return Status();
    }

    *wt = new TableScan(t);
    return Status();
}

Status Parser::ParseBinaryScan(Scan** wt) {
    Scan* left = ParseScan(ParsePrimaryScan);

    while (PeekToken().type == TokenType::Cross || 
           PeekToken().type == TokenType::Inner ||
           PeekToken().type == TokenType::Left ||
           PeekToken().type == TokenType::Right ||
           PeekToken().type == TokenType::Full) {

        switch (NextToken().type) {
            case TokenType::Cross: {
                EatToken(TokenType::Join, "Parse Error: Expected keyword 'join' after keyword 'cross'");
                Scan* right = ParseScan(ParsePrimaryScan);
                *wt = new ProductScan(left, right);
                return Status();
            }
            case TokenType::Inner: {
                EatToken(TokenType::Join, "Parse Error: Expected keyword 'join' after keyword 'inner'");
                Scan* right = ParseScan(ParsePrimaryScan);
                EatToken(TokenType::On, "Parse Error: Expected 'on' keyword and join predicate for inner joins");
                Expr* on = ParseExpr(Base);
                *wt = new SelectScan(new ProductScan(left, right), on);
                return Status();
            }
            case TokenType::Left: {
                EatToken(TokenType::Join, "Parse Error: Expected keyword 'join' after keyword 'left'");
                Scan* right = ParseScan(ParsePrimaryScan);
                EatToken(TokenType::On, "Parse Error: Expected 'on' keyword and join predicate for left joins");
                Expr* on = ParseExpr(Base);
                *wt = new OuterSelectScan(new ProductScan(left, right), on, true, false);
                return Status();
            }
            case TokenType::Right: {
                EatToken(TokenType::Join, "Parse Error: Expected keyword 'join' after keyword 'right'");
                Scan* right = ParseScan(ParsePrimaryScan);
                EatToken(TokenType::On, "Parse Error: Expected 'on' keyword and join predicate for right joins");
                Expr* on = ParseExpr(Base);
                *wt = new OuterSelectScan(new ProductScan(left, right), on, false, true);
                return Status();
            }
            case TokenType::Full: {
                EatToken(TokenType::Join, "Parse Error: Expected keyword 'join' after keyword 'full'");
                Scan* right = ParseScan(ParsePrimaryScan);
                EatToken(TokenType::On, "Parse Error: Expected 'on' keyword and join predicate for full joins");
                Expr* on = ParseExpr(Base);
                *wt = new OuterSelectScan(new ProductScan(left, right), on, true, true);
                return Status();
            }
            default:
                return Status();
        }
    }

    *wt = left;
    return Status();
}

Status Parser::ParseBaseScan(Scan** wt) {
    *wt = ParseScan(ParseBinaryScan);
    return Status();
}


Status Parser::ParseStmt(Stmt** stmt) {
    Token next = NextToken();
    switch (next.type) {
        case TokenType::Create: {
            if (PeekToken().type == TokenType::Table) {
                EatToken(TokenType::Table, "Parse Error: Expected 'table' keyword after 'create' keyword");
                Token target = EatToken(TokenType::Identifier, "Parse Error: Expected table name after 'table' keyword");
                EatToken(TokenType::LParen, "Parse Error: Expected '(' after table name");

                std::vector<Token> names;
                std::vector<Token> types;
                std::vector<bool> not_null_constraints;
                std::vector<Token> pks;
                std::vector<std::vector<Token>> uniques;
                std::vector<bool> nulls_distinct;

                while (!AdvanceIf(TokenType::RParen)) {
                    if (AdvanceIf(TokenType::Primary)) {
                        if (!pks.empty())
                            return Status(false, "Parse Error: Only one primary key constraint is allowed");

                        EatToken(TokenType::Key, "Parse Error: Expected keyword 'key' after keyword 'primary'");
                        EatToken(TokenType::LParen, "Parse Error: Expected '(' before primary key columns");

                        while (!AdvanceIf(TokenType::RParen)) {
                            Token col = EatToken(TokenType::Identifier, "Parse Error: Expected column name as primary key");
                            pks.push_back(col);
                            AdvanceIf(TokenType::Comma);
                        }

                    } else if (AdvanceIf(TokenType::Unique)) {
                        EatToken(TokenType::LParen, "Parse Error: Expected '(' before unqiue columns");

                        std::vector<Token> cols;
                        while (!AdvanceIf(TokenType::RParen)) {
                            Token col = EatToken(TokenType::Identifier, "Parse Error: Expected column name as unique column");
                            cols.push_back(col);
                            AdvanceIf(TokenType::Comma);
                        }

                        EatToken(TokenType::Nulls, "Parse Error: 'nulls distinct' or 'nulls not distinct' must be included");
                        Token distinct_clause_token = EatTokenIn(std::vector<TokenType>({TokenType::Not, TokenType::Distinct}), 
                                                                 "Parse Error: Expected 'nulls distinct' or 'nulls not distinct'");
                        if (distinct_clause_token.type == TokenType::Not)
                            EatToken(TokenType::Distinct, "Parse Error: Expected keyword 'distinct' after 'not'");

                        uniques.push_back(cols);
                        nulls_distinct.push_back(distinct_clause_token.type == TokenType::Distinct);
                    } else {
                        names.push_back(EatToken(TokenType::Identifier, "Parse Error: Expected column name"));
                        types.push_back(EatTokenIn(TokenTypeSQLDataTypes(), "Parse Error: Expected valid SQL data type"));

                        if (AdvanceIf(TokenType::Not)) {
                            EatToken(TokenType::Null, "Parse Error: 'not' keyword must be followed by 'null'");
                            not_null_constraints.push_back(true);
                        } else {
                            not_null_constraints.push_back(false);
                        }
                    }


                    AdvanceIf(TokenType::Comma);
                }

                EatToken(TokenType::SemiColon, "Parse Error: Expected ';' after query");
                *stmt = new CreateStmt(target, names, types, not_null_constraints, pks, uniques, nulls_distinct);
                return Status();
            }

            //Creating a model
            EatToken(TokenType::Model, "Parse Error: Expected 'model' or 'table' after 'create' keyword");
            Token name = EatToken(TokenType::Identifier, "Parse Error: Expected model name after 'model' keyword");
            EatToken(TokenType::LParen, "Parse Error: Expected '(' before model path");
            Token path = NextToken();
            EatToken(TokenType::RParen, "Parse Error: Expected ')' after model output transformation pathname");
            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' after query");
            *stmt = new CreateModelStmt(name, path);
            return Status();
        }
        case TokenType::Insert: {
            EatToken(TokenType::Into, "Parse Error: Expected 'into' keyword after 'insert'");
            Scan* target = ParseScan(ParseBaseScan);

            std::vector<Column> cols;
            EatToken(TokenType::LParen, "Parse Error: Expected '(' and columns names for insert statements");
            while (!AdvanceIf(TokenType::RParen)) {
                if (PeekToken().type != TokenType::Identifier)
                    return Status(false, "Parse Error: Expected column name");
                Column col = ParseColumn();
                cols.push_back(col);
                AdvanceIf(TokenType::Comma);
            }

            EatToken(TokenType::Values, "Parse Error: Expected 'values' keyword");
            std::vector<std::vector<Expr*>> assigns;
            while (AdvanceIf(TokenType::LParen)) {
                std::vector<Expr*> tuple;

                int i = 0;
                while (!AdvanceIf(TokenType::RParen)) {
                    Expr* value = ParseExpr(Base);
                    tuple.push_back(new ColAssign(cols.at(i), value));
                    AdvanceIf(TokenType::Comma);
                    i++;
                }

                assigns.push_back(tuple);

                AdvanceIf(TokenType::Comma);
            }
            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of insert statement");
            *stmt = new InsertStmt(target, assigns);
            return Status();
        }
        case TokenType::Select: {
            bool remove_duplicates = AdvanceIf(TokenType::Distinct) ? true : false;

            std::vector<Expr*> target_cols;
            do {
                Expr* col = ParseExpr(Base);
                target_cols.push_back(col);
            } while (AdvanceIf(TokenType::Comma));

            Scan* target = AdvanceIf(TokenType::From) ? ParseScan(ParseBaseScan) : new ConstantScan(target_cols);

            Expr* where_clause = AdvanceIf(TokenType::Where) ? ParseExpr(Base) : nullptr;

            if (where_clause) {
                target = new SelectScan(target, where_clause);
            }

            std::vector<Expr*> group_cols;
            if (AdvanceIf(TokenType::Group)) {
                EatToken(TokenType::By, "Parse Error: Expected keyword 'by' after keyword 'group'");
                do {
                    Expr* col = ParseExpr(Base);
                    group_cols.push_back(col);
                } while (AdvanceIf(TokenType::Comma));
            }

            Expr* having_clause = AdvanceIf(TokenType::Having) ? ParseExpr(Base) : nullptr;

            std::vector<OrderCol> order_cols;
            if (AdvanceIf(TokenType::Order)) {
                EatToken(TokenType::By, "Parse Error: Expected keyword 'by' after keyword 'order'");

                do {
                    Expr* col = ParseExpr(Base);
                    Token asc = EatTokenIn(std::vector<TokenType>({TokenType::Asc, TokenType::Desc}), 
                                           "Parse Error: Expected either keyword 'asc' or 'desc' after column name");
                    order_cols.push_back({col, asc.type == TokenType::Asc ? new Literal(true) : new Literal(false)});
                } while (AdvanceIf(TokenType::Comma));
            }

            Expr* limit = AdvanceIf(TokenType::Limit) ? ParseExpr(Base) : new Literal(-1);

            //if the select statement is a subquery, it will not end with a semicolon
            AdvanceIf(TokenType::SemiColon);

            Scan* project_scan = new ProjectScan(target, target_cols, group_cols, having_clause, order_cols, limit, remove_duplicates);
            *stmt = new SelectStmt(project_scan);
            return Status();
        }

        case TokenType::Update: {
            Scan* target = ParseScan(ParseBaseScan);
            EatToken(TokenType::Set, "Parse Error: Expected keyword 'set' after table name");

            std::vector<Expr*> assigns;
            while (!(PeekToken().type == TokenType::SemiColon || PeekToken().type == TokenType::Where)) {
                if (PeekToken().type != TokenType::Identifier)
                    return Status(false, "Parse Error: Expected column identifier");

                Column col = ParseColumn();
                EatToken(TokenType::Equal, "Parse Error: Expected '=' after column name");
                Expr* value = ParseExpr(Base);
                assigns.push_back(new ColAssign(col, value));

                AdvanceIf(TokenType::Comma);
            }

            Expr* where_clause = AdvanceIf(TokenType::Where) ? ParseExpr(Base) : nullptr;

            if (where_clause) {
                *stmt = new UpdateStmt(assigns, new SelectScan(target, where_clause));
            } else {
                *stmt = new UpdateStmt(assigns, target);
            }

            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of update statement");

            return Status();
        }
        case TokenType::Delete: {
            EatToken(TokenType::From, "Parse Error: Expected 'from' keyword after 'delete'");
            Token target = EatToken(TokenType::Identifier, "Parse Error: Expected table name after keyword 'from'");

            Expr* where_clause = AdvanceIf(TokenType::Where) ? ParseExpr(Base) : nullptr;

            if (where_clause) {
                *stmt = new DeleteStmt(new SelectScan(new TableScan(target), where_clause));
            } else {
                *stmt = new DeleteStmt(new TableScan(target));
            }

            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of delete statement");

            return Status();
        }
        case TokenType::Drop: {
            Token t = EatTokenIn(std::vector<TokenType>({TokenType::Null, TokenType::Not}), 
                                 "Parse Error: Expected keyword 'table' or 'model' after 'drop'");

            if (t.type == TokenType::Table) {
                bool has_if_exists = false;
                if (AdvanceIf(TokenType::If)) {
                    has_if_exists = true;
                    EatToken(TokenType::Exists, "Parse Error: Expected keyword 'exists' after 'if'");
                }
                Token target = EatToken(TokenType::Identifier, "Parse Error: Expected table name");
                EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of drop table statement");

                *stmt = new DropTableStmt(target, has_if_exists);
                return Status();
            } else if (t.type == TokenType::Model) {
                bool has_if_exists = false;
                if (AdvanceIf(TokenType::If)) {
                    has_if_exists = true;
                    EatToken(TokenType::Exists, "Parse Error: Expected keyword 'exists' after 'if'");
                }
                Token target = EatToken(TokenType::Identifier, "Parse Error: Expected model name");
                EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of drop model statement");

                *stmt = new DropModelStmt(target, has_if_exists);
                return Status();
            }
        }
        case TokenType::Describe: {
            EatToken(TokenType::Table, "Parse Error: Expected keyword 'table' after 'describe'");
            Token target = EatToken(TokenType::Identifier, "Parse Error: Expected table name");
            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of describe statement");

            *stmt = new DescribeTableStmt(target);
            return Status();
        }
        case TokenType::Begin:
        case TokenType::Commit:
        case TokenType::Rollback:
            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of describe statement");
            *stmt = new TxnControlStmt(next);
            return Status();
        default:
            return Status(false, "Parse Error: Invalid token");
    }

    //should never reach this branch
    return Status(false, "Parse Error: Invalid token");
}

}
