#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <memory>

namespace wsldb {

class Matcher {
public:
    Matcher(const std::string& pattern) {
        nfa_ = CompilePattern(pattern);
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
    public:
        bool is_end;
        std::unordered_map<char, std::shared_ptr<NfaState>> symbol_transition; //could replace this with an std::pair
        std::vector<std::shared_ptr<NfaState>> epsilon_transitions;
    };

    struct Nfa {
        std::shared_ptr<NfaState> start;
        std::shared_ptr<NfaState> end;
    };

    void AddEpsilonTransition(std::shared_ptr<NfaState> from, std::shared_ptr<NfaState> to) {
        from->epsilon_transitions.push_back(to);
    }

    void AddSymbolTransition(std::shared_ptr<NfaState> from, std::shared_ptr<NfaState> to, char symbol) {
        from->symbol_transition.insert({ symbol, to });
    }

    Nfa MakeEpsilon() {
        std::shared_ptr<NfaState> start = std::make_shared<NfaState>(false);
        std::shared_ptr<NfaState> end = std::make_shared<NfaState>(true);
        AddEpsilonTransition(start, end);
        return { start, end };
    }

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

    //TODO: MakeUnion

    Nfa CompilePattern(const std::string& pattern) {
        size_t idx = 0;
        Nfa root = CompileBase(pattern, &idx);
        while (!AtEnd(pattern, idx)) {
            root = MakeConcat(root, CompileBase(pattern, &idx));
        }
        return root;
    }

    Nfa CompileBase(const std::string& pattern, size_t* idx) {
        char c = pattern.at((*idx)++);
        switch (c) {
            case '%':   return MakeClosure(MakeSymbol('_'));
            case '_':   return MakeSymbol('_');
            default:    return MakeSymbol(c);
        }
    }

    bool AtEnd(const std::string& pattern, size_t idx) {
        return idx >= pattern.size();
    }

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
