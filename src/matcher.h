#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <memory>

namespace wsldb {

class Matcher {
public:
    enum class Type {
        Like,
        Similar
    };

    Matcher(Type type, const std::string& pattern, bool* status) {
        Compiler c(pattern);
        switch (type) {
            case Type::Like:
                *status = c.CompileForLike(&nfa_);
                break;
            case Type::Similar:
                *status = c.CompileForSimilar(&nfa_);
                break;
            default:
                *status = false;
                break;
        }
    }

    bool Match(const std::string& str) {
        std::vector<NfaState*> current_states;
        std::vector<NfaState*> visited;
        AddNextState(nfa_.start, current_states, visited);

        for (const char& c: str) {
            std::vector<NfaState*> next_states;
            for (NfaState* s: current_states) {
                std::unordered_map<char, std::shared_ptr<NfaState>>::iterator it = s->symbol_transition.find(c);
                std::unordered_map<char, std::shared_ptr<NfaState>>::iterator it_any = s->symbol_transition.find('_');
                std::vector<NfaState*> placeholder;
                if (it != s->symbol_transition.end()) {
                    AddNextState(it->second, next_states, placeholder);
                } else if (it_any != s->symbol_transition.end()) {
                    AddNextState(it_any->second, next_states, placeholder);
                }
            }
            current_states = next_states;
        }

        for (NfaState* s: current_states) {
            if (s->is_end) return true;
        }

        return false;
    }

private:
    struct NfaState {
    public:
        NfaState(bool is_end): is_end(is_end) {}
        NfaState(bool is_end, std::unordered_map<char, std::shared_ptr<NfaState>> st, std::vector<std::shared_ptr<NfaState>> et):
            is_end(is_end), symbol_transition(st), epsilon_transitions(et) {}
    public:
        bool is_end;
        std::unordered_map<char, std::shared_ptr<NfaState>> symbol_transition;
        std::vector<std::shared_ptr<NfaState>> epsilon_transitions;
    };

    struct Nfa {
        std::shared_ptr<NfaState> start;
        std::shared_ptr<NfaState> end;

        std::shared_ptr<NfaState> AddState(std::shared_ptr<NfaState> ptr, std::unordered_map<std::shared_ptr<NfaState>, std::shared_ptr<NfaState>>& map) {
            if (map.find(ptr) != map.end())
                return map.at(ptr);

            std::shared_ptr<NfaState> clone = std::make_shared<NfaState>(ptr->is_end);

            map.insert({ ptr, clone });
            for (const std::pair<const char, std::shared_ptr<NfaState>>& p: ptr->symbol_transition) {
                std::shared_ptr<NfaState> child = AddState(p.second, map);
                clone->symbol_transition.insert({ p.first, child });
            }
            for (std::shared_ptr<NfaState> p: ptr->epsilon_transitions) {
                std::shared_ptr<NfaState> child = AddState(p, map);
                clone->epsilon_transitions.push_back(child);
            }

            return clone;
        }
        Nfa Clone() {
            std::unordered_map<std::shared_ptr<NfaState>, std::shared_ptr<NfaState>> map;
            std::shared_ptr<NfaState> cloned_start = AddState(start, map);
            return { cloned_start, map.at(end) };
        }
    };

    class Compiler {
    public:
        Compiler(const std::string& pattern): pattern_(pattern) {}
        bool CompileForLike(Nfa* nfa) {
            if (!CompileLikeBase(nfa))
                return false;
            while (!AtEnd()) {
                Nfa other;
                if (!CompileLikeBase(&other))
                    return false;
                *nfa = MakeConcat(*nfa, other);
            }
            return true;
        }
        bool CompileForSimilar(Nfa* nfa) {
            if (!CompileAlternation(nfa))
                return false;
            while (!AtEnd()) {
                Nfa other;
                if (!CompileAlternation(&other))
                    return false;
                *nfa = MakeConcat(*nfa, other);
            }
            return true;
        }
    private:
        //functions for 'like' matching
        bool CompileLikeBase(Nfa* nfa) {
            char c = NextChar();
            switch (c) {
                case '%':   *nfa = MakeClosure(MakeSymbol('_')); return true;
                case '_':   *nfa = MakeSymbol('_'); return true;
                default:    *nfa = MakeSymbol(c); return true;
            }
        }

        //functions for 'similar to' matching
        bool CompileAlternation(Nfa* nfa) {
            if (!CompileConcat(nfa))
                return false;
            while (PeekChar('|')) {
                NextChar();
                Nfa other;
                if (!CompileConcat(&other))
                    return false;
                *nfa = MakeUnion(*nfa, other);
            }
            return true;
        }
        bool CompileConcat(Nfa* nfa) {
            if (!CompileDuplication(nfa))
                return false;
            while (!AtEnd() && !PeekMetaChar()) {
                Nfa other;
                if (!CompileDuplication(&other))
                    return false;
                *nfa = MakeConcat(*nfa, other);
            }
            return true;
        }
        bool CompileDuplication(Nfa* nfa) {
            if (!CompileAtomic(nfa))
                return false;
            while (PeekDupChar()) {
                char next = NextChar();
                switch (next) {
                    case '*':
                        *nfa = MakeClosure(*nfa);
                        break;
                    case '+':
                        *nfa = MakeOneOrMore(*nfa);
                        break;
                    case '?':
                        *nfa = MakeZeroOrOne(*nfa);
                        break;
                    case '{': {
                        int m;
                        if (!ParseInt(&m)) return false;

                        if (PeekChar('}')) {
                            *nfa = MakeExactlyM(*nfa, m);
                        } else if (PeekChar(',')) {
                            NextChar(); //,
                            if (PeekChar('}')) {
                                *nfa = MakeMOrMore(*nfa, m);
                            } else { //next is integer
                                int n;
                                if (!ParseInt(&n)) return false;
                                *nfa = MakeMToN(*nfa, m, n);
                            }
                        }
                        if (!EatChar('}')) return false;
                        break;
                    }
                    default:
                        return false;
                        break;
                }
            }
            return true;
        }
        bool CompileAtomic(Nfa* nfa) {
            switch (PeekChar()) {
                case '(': {
                    NextChar();
                    size_t start = idx_;
                    while (!PeekChar(')')) {
                        NextChar();
                    }
                    size_t end = idx_;
                    NextChar();
                    Compiler c(pattern_.substr(start, end - start));
                    return c.CompileForSimilar(nfa);
                }
                case '%': {
                    NextChar();
                    *nfa = MakeClosure(MakeSymbol('_')); return true;
                }
                case '_': {
                    NextChar();
                    *nfa = MakeSymbol('_'); return true;
                }
                default: {
                    if (PeekDupChar() || PeekChar('|'))
                        return false;
                    *nfa = MakeSymbol(NextChar()); return true;
                }
            }  
        }
        bool PeekMetaChar() {
            return PeekChar('|') ||
                   PeekChar('*') ||
                   PeekChar('_') ||
                   PeekChar('%') ||
                   PeekChar('+') ||
                   PeekChar('?') ||
                   PeekChar('(') ||
                   PeekChar(')');
        }

        bool PeekDupChar() {
            return PeekChar('*') ||
                   PeekChar('+') ||
                   PeekChar('?') ||
                   PeekChar('{');
        }
        bool PeekChar(char c) {
            return !AtEnd() && pattern_.at(idx_) == c;
        }
        char PeekChar() {
            return pattern_.at(idx_);
        }
        bool EatChar(char c) {
            return NextChar() == c;
        }
        char NextChar() {
            return pattern_.at(idx_++);
        }
        bool ParseInt(int* result) {
            size_t start = idx_;
            while (!AtEnd() && IsNumeric(PeekChar())) {
                NextChar();
            }

            std::string num = pattern_.substr(start, idx_ - start);
            if (num.size() == 0) return false;

            *result = std::stoi(num);
            return true;
        }
        bool IsNumeric(char c) {
            return '0' <= c && c <= '9';
        }
        bool AtEnd() {
            return idx_ >= pattern_.size();
        }

        //functions for NFAs
        void AddEpsilonTransition(std::shared_ptr<NfaState> from, std::shared_ptr<NfaState> to) {
            from->epsilon_transitions.push_back(to);
        }

        void AddSymbolTransition(std::shared_ptr<NfaState> from, std::shared_ptr<NfaState> to, char symbol) {
            from->symbol_transition.insert({ symbol, to });
        }

        //TODO: look at this - not really used in the way we are compiling right now
        /*
        Nfa MakeEpsilon() {
            std::shared_ptr<NfaState> start = std::make_shared<NfaState>(false);
            std::shared_ptr<NfaState> end = std::make_shared<NfaState>(true);
            AddEpsilonTransition(start, end);
            return { start, end };
        }*/

        Nfa MakeSymbol(char symbol) {
            std::shared_ptr<NfaState> start = std::make_shared<NfaState>(false);
            std::shared_ptr<NfaState> end = std::make_shared<NfaState>(true);
            AddSymbolTransition(start, end, symbol);
            return { start, end };
        }
        Nfa MakeConcat(Nfa first, Nfa second) {
            AddEpsilonTransition(first.end, second.start);
            first.end->is_end = false;
            return { first.start, second.end };
        }
        Nfa MakeClosure(Nfa nfa) {
            std::shared_ptr<NfaState> start = std::make_shared<NfaState>(false);
            std::shared_ptr<NfaState> end = std::make_shared<NfaState>(true);

            AddEpsilonTransition(start, end);
            AddEpsilonTransition(start, nfa.start);

            AddEpsilonTransition(nfa.end, end);
            AddEpsilonTransition(nfa.end, nfa.start);
            nfa.end->is_end = false;
            return { start, end };
        }
        Nfa MakeUnion(Nfa first, Nfa second) {
            std::shared_ptr<NfaState> start = std::make_shared<NfaState>(false);
            AddEpsilonTransition(start, first.start);
            AddEpsilonTransition(start, second.start);

            std::shared_ptr<NfaState> end = std::make_shared<NfaState>(true);
            AddEpsilonTransition(first.end, end);
            first.end->is_end = false;
            AddEpsilonTransition(second.end, end);
            second.end->is_end = false;

            return { start, end };
        }
        Nfa MakeOneOrMore(Nfa nfa) {
            return MakeConcat(nfa.Clone(), MakeClosure(nfa.Clone()));
        }
        Nfa MakeZeroOrOne(Nfa nfa) {
            std::shared_ptr<NfaState> start = std::make_shared<NfaState>(false);
            std::shared_ptr<NfaState> end = std::make_shared<NfaState>(true);

            AddEpsilonTransition(start, end);
            AddEpsilonTransition(start, nfa.start);

            AddEpsilonTransition(nfa.end, end);
            nfa.end->is_end = false;
            return { start, end };
        }
        Nfa MakeExactlyM(Nfa nfa, int m) {
            Nfa result = nfa.Clone();

            for (int i = 0; i < m - 1; i++) {
                result = MakeConcat(result, nfa.Clone());
            }

            return result;
        }
        Nfa MakeMOrMore(Nfa nfa, int m) {
            return MakeConcat(MakeExactlyM(nfa.Clone(), m), MakeClosure(nfa.Clone()));
        }
        Nfa MakeMToN(Nfa nfa, int m, int n) {
            Nfa result = MakeExactlyM(nfa.Clone(), m);

            for (int i = 0; i < n - m; i++) {
                result = MakeConcat(result, MakeZeroOrOne(nfa.Clone())); 
            }

            return result;
        }

    private:
        std::string pattern_;
        size_t idx_ {0};
    };    

    void AddNextState(std::shared_ptr<NfaState> state, std::vector<NfaState*>& next_states, std::vector<NfaState*>& visited_states) {
        if (!state->epsilon_transitions.empty()) {
            for (std::shared_ptr<NfaState> s: state->epsilon_transitions) {
                if (std::find(visited_states.begin(), visited_states.end(), s.get()) == visited_states.end()) {
                    visited_states.push_back(s.get());
                    AddNextState(s, next_states, visited_states);
                }
            }
        } else {
            next_states.push_back(state.get());
        }
    }
private:
    Nfa nfa_;
};

}
