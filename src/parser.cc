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
        default:
            return NULL;
    }
    return NULL;
}

Expr* Parser::ParseRelational() {
    Expr* left = ParsePrimary();

    while (PeekToken().type == TokenType::Less ||
           PeekToken().type == TokenType::LessEqual ||
           PeekToken().type == TokenType::Greater ||
           PeekToken().type == TokenType::GreaterEqual) {
        Token op = NextToken();
        left = new Binary(op, left, ParsePrimary());
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

Expr* Parser::ParseExpr() {
    return ParseEquality();
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
            }
            NextToken(); //;
            return new SelectStmt(target, target_cols, where_clause);
        }
    }

    return NULL;
}

}
