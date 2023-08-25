#include <vector>

#include "parser.h"
#include "tuple.h"

namespace wsldb {


std::vector<Stmt*> Parser::ParseStmts() {
    std::vector<Stmt*> stmts = std::vector<Stmt*>();

    while (PeekToken().type != TokenType::Eof) {
        stmts.push_back(ParseStmt());    
    }
    
    return stmts;
}

Expr* Parser::ParsePrimaryExpr() {
    switch (PeekToken().type) {
        case TokenType::IntLiteral:
            return new IntLiteral(NextToken());
        case TokenType::StringLiteral:
            return new StringLiteral(NextToken());
        default:
            return NULL;
    }
    return NULL;
}

Expr* Parser::ParseExpr() {
    return ParsePrimaryExpr();
}

Tuple* Parser::ParseTuple() {
    NextToken(); //(
    std::vector<Expr*> exprs = std::vector<Expr*>();
    while (PeekToken().type != TokenType::RParen) {
        exprs.push_back(ParseExpr());
        if (PeekToken().type == TokenType::Comma) {
            NextToken(); //,
        }
    }
    NextToken(); //)
    return new Tuple(exprs);
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
            std::vector<Tuple*> values = std::vector<Tuple*>();
            while (PeekToken().type == TokenType::LParen) {
                values.push_back(ParseTuple());
                if (PeekToken().type == TokenType::Comma) {
                    NextToken(); //,
                }
            }
            NextToken(); //;
            return new InsertStmt(target, values);
        }
    }

    return NULL;
}

}
