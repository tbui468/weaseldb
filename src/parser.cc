#include <vector>
#include <algorithm>

#include "parser.h"

namespace wsldb {

#define EatToken(expected_type, err_msg) \
    ({ Token t = NextToken(); \
       if (t.type != expected_type) return Status(false, err_msg); \
       t; })

#define EatTokenIn(tv, err_msg) \
    ({ Token t = NextToken(); \
       if (std::find(tv.begin(), tv.end(), t.type) == tv.end()) return Status(false, err_msg); \
       t; })

Status Parser::ParseTxns(std::vector<Txn>& txns) {
    while (!AdvanceIf(TokenType::Eof)) {
        Txn txn;
        Status s = ParseTxn(&txn);
        if (!s.Ok())
            return s;

        txns.push_back(txn);
    }

    return Status();
}

Status Parser::ParseTxn(Txn* txn) {
    std::vector<Stmt*> stmts;

    bool single_stmt_txn = true;
    bool commit_on_success = true;
    if (AdvanceIf(TokenType::Begin)) {
        single_stmt_txn = false;
        EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at the end of begin statement");
    }

    while (!AdvanceIf(TokenType::Eof)) {
        if (PeekToken().type == TokenType::Commit || PeekToken().type == TokenType::Rollback) {
            if (NextToken().type == TokenType::Rollback) { //rollback or commit
                commit_on_success = false;
            }
            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at the end of statement");
            break;
        }
        Stmt* stmt;
        Status s = ParseStmt(&stmt);
        if (!s.Ok())
            return s;

        stmts.push_back(stmt);
        if (single_stmt_txn)
            break;
    }

    *txn = {stmts, commit_on_success};
    return Status();
}

Expr* Parser::ParsePrimary() {
    switch (PeekToken().type) {
        case TokenType::IntLiteral:
        case TokenType::FloatLiteral:
        case TokenType::StringLiteral:
        case TokenType::TrueLiteral:
        case TokenType::FalseLiteral:
        case TokenType::Null:
        case TokenType::ByteaLiteral:
            return new Literal(NextToken());
        case TokenType::Identifier: {
            Token ref = NextToken();
            if (AdvanceIf(TokenType::Dot)) {
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
           PeekToken().type == TokenType::NotEqual ||
           PeekToken().type == TokenType::Is) {

        Token op = NextToken();
        if (op.type == TokenType::Is) {
            Token t = NextToken();
            if (t.type == TokenType::Null) {
                left = new IsNull(left);
            } else { //t.type must be 'not'
                NextToken(); //null
                left = new IsNotNull(left);
            } 
        } else {
            left = new Binary(op, left, ParseRelational());
        }
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
    if (PeekToken().type == TokenType::Select) {
        Stmt* stmt;
        Status s = ParseStmt(&stmt);
        return new ScalarSubquery(stmt);
    }

    return ParseOr();
}

WorkTable* Parser::ParsePrimaryWorkTable() {
    Token t = NextToken();
    if (AdvanceIf(TokenType::As)) {
        Token alias = NextToken();
        return new PrimaryTable(t, alias);
    }

    return new PrimaryTable(t);
}

WorkTable* Parser::ParseBinaryWorkTable() {
    WorkTable* left = ParsePrimaryWorkTable();
    while (PeekToken().type == TokenType::Cross || 
           PeekToken().type == TokenType::Inner ||
           PeekToken().type == TokenType::Left ||
           PeekToken().type == TokenType::Right ||
           PeekToken().type == TokenType::Full) {

        switch (NextToken().type) {
            case TokenType::Cross: {
                NextToken(); //join
                left = new CrossJoin(left, ParsePrimaryWorkTable());
                break;
            }
            case TokenType::Inner: {
                NextToken(); //join
                WorkTable* right = ParsePrimaryWorkTable();
                NextToken(); //on
                left = new InnerJoin(left, right, ParseExpr());
                break;
            }
            case TokenType::Left: {
                NextToken(); //join
                WorkTable* right = ParsePrimaryWorkTable();
                NextToken(); //on
                left = new LeftJoin(left, right, ParseExpr());
                break;
            }
            case TokenType::Right: {
                NextToken(); //join
                WorkTable* right = ParsePrimaryWorkTable();
                NextToken(); //on
                left = new LeftJoin(right, left, ParseExpr());
                break;
            }
            case TokenType::Full: {
                NextToken(); //join
                WorkTable* right = ParsePrimaryWorkTable();
                NextToken(); //on
                left = new FullJoin(left, right, ParseExpr());
                break;
            }
            default:
                break;
        }
    }

    return left;
}

WorkTable* Parser::ParseWorkTable() {
    return ParseBinaryWorkTable();
}


Status Parser::ParseStmt(Stmt** stmt) {
    switch (NextToken().type) {
        case TokenType::Create: {
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
        case TokenType::Insert: {
            EatToken(TokenType::Into, "Parse Error: Expected 'into' keyword after 'insert'");
            Token target = EatToken(TokenType::Identifier, "Parse Error: Expected table name after 'into' keyword");

            std::vector<Token> cols;
            EatToken(TokenType::LParen, "Parse Error: Expected '(' and columns names for insert statements");
            while (!AdvanceIf(TokenType::RParen)) {
                cols.push_back(EatToken(TokenType::Identifier, "Parse Error: Expected column name"));
                AdvanceIf(TokenType::Comma);
            }

            EatToken(TokenType::Values, "Parse Error: Expected 'values' keyword");
            std::vector<std::vector<Expr*>> values;
            while (AdvanceIf(TokenType::LParen)) {
                std::vector<Expr*> tuple;

                while (!AdvanceIf(TokenType::RParen)) {
                    tuple.push_back(ParseExpr()); //TODO: ParseExpr should return a status, and need to check it for error
                    AdvanceIf(TokenType::Comma);
                }

                values.push_back(tuple);

                AdvanceIf(TokenType::Comma);
            }
            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of insert statement");
            *stmt = new InsertStmt(target, cols, values);
            return Status();
        }
        case TokenType::Select: {
            bool remove_duplicates = false;
            if (AdvanceIf(TokenType::Distinct)) {
                remove_duplicates = true;
            }

            std::vector<Expr*> target_cols;
            do {
                target_cols.push_back(ParseExpr()); //TODO: check if ParseExpr returns an error
            } while (AdvanceIf(TokenType::Comma));

            WorkTable* target = nullptr;
            if (AdvanceIf(TokenType::From)) {
                target = ParseWorkTable(); //TODO: Check if ParseWorkTable returns error
            } else {
                target = new ConstantTable(target_cols);
            }

            Expr* where_clause = nullptr;
            if (AdvanceIf(TokenType::Where)) {
                where_clause = ParseExpr();  //TODO: Check if ParseExpr returns error
            } else {
                where_clause = new Literal(true);
            }

            std::vector<OrderCol> order_cols;
            if (AdvanceIf(TokenType::Order)) {
                EatToken(TokenType::By, "Parse Error: Expected keyword 'by' after keyword 'order'");

                do {
                    Expr* col = ParseExpr(); //TODO: Check if ParseExpr returns error
                    Token asc = EatTokenIn(std::vector<TokenType>({TokenType::Asc, TokenType::Desc}), 
                                           "Parse Error: Expected either keyword 'asc' or 'desc' after column name");
                    order_cols.push_back({col, asc.type == TokenType::Asc ? new Literal(true) : new Literal(false)});
                } while (AdvanceIf(TokenType::Comma));
            }

            Expr* limit = nullptr;
            if (AdvanceIf(TokenType::Limit)) {
                limit = ParseExpr(); //TODO: Check if ParseExpr returns error
            } else {
                limit = new Literal(-1); //no limit
            } 

            //if the select statement is a subquery, it will not end with a semicolon
            AdvanceIf(TokenType::SemiColon);

            *stmt = new SelectStmt(target, target_cols, where_clause, order_cols, limit, remove_duplicates);
            return Status();
        }

        case TokenType::Update: {
            Token target = EatToken(TokenType::Identifier, "Parse Error: Expected table name");
            EatToken(TokenType::Set, "Parse Error: Expected keyword 'set' after table name");

            std::vector<Expr*> assigns;
            while (!(PeekToken().type == TokenType::SemiColon || PeekToken().type == TokenType::Where)) {
                Token col = EatToken(TokenType::Identifier, "Parse Error: Expected column name");
                EatToken(TokenType::Equal, "Parse Error: Expected '=' after column name");
                assigns.push_back(new ColAssign(col, ParseExpr())); //TODO: Check for ParseExpr error

                AdvanceIf(TokenType::Comma);
            }

            Expr* where_clause = nullptr;
            if (AdvanceIf(TokenType::Where)) {
                where_clause = ParseExpr();  //TODO: Check for ParseExpr error
            } else {
                where_clause = new Literal(true);
            }
            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of update statement");

            *stmt = new UpdateStmt(target, assigns, where_clause);
            return Status();
        }
        case TokenType::Delete: {
            EatToken(TokenType::From, "Parse Error: Expected 'from' keyword after 'delete'");
            Token target = EatToken(TokenType::Identifier, "Parse Error: Expected table name after keyword 'from'");

            Expr* where_clause = nullptr;
            if (AdvanceIf(TokenType::Where)) {
                where_clause = ParseExpr();  //TODO: Check for ParseExpr error
            } else {
                where_clause = new Literal(true);
            }
            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of delete statement");

            *stmt = new DeleteStmt(target, where_clause);
            return Status();
        }
        case TokenType::Drop: {
            EatToken(TokenType::Table, "Parse Error: Expected keyword 'table' after 'drop'");
            bool has_if_exists = false;
            if (AdvanceIf(TokenType::If)) {
                has_if_exists = true;
                EatToken(TokenType::Exists, "Parse Error: Expected keyword 'exists' after 'if'");
            }
            Token target = EatToken(TokenType::Identifier, "Parse Error: Expected table name");
            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of drop statement");

            *stmt = new DropTableStmt(target, has_if_exists);
            return Status();
        }
        case TokenType::Describe: {
            EatToken(TokenType::Table, "Parse Error: Expected keyword 'table' after 'describe'");
            Token target = EatToken(TokenType::Identifier, "Parse Error: Expected table name");
            EatToken(TokenType::SemiColon, "Parse Error: Expected ';' at end of describe statement");

            *stmt = new DescribeTableStmt(target);
            return Status();
        }
        default:
            return Status(false, "Parse Error: Invalid token");
    }

    //should never reach this branch
    return Status(false, "Parse Error: Invalid token");
}

}
