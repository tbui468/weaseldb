#pragma once

#include <torch/torch.h>
#include <memory>
#include <vector>
#include <fstream>

namespace wsldb {

class CustomDataset : public torch::data::Dataset<CustomDataset> {
public:
    explicit CustomDataset(const std::string& data_path, const std::string& target_path) {
        {
            std::fstream f;
            f.open(target_path, std::ios::in | std::ios::binary);
            int32_t magic = nextint32(f);
            int32_t count = nextint32(f);
            target_ = torch::empty({count}, torch::kByte);
            f.read(reinterpret_cast<char*>(target_.data_ptr()), target_.numel());
        }

        {
            std::fstream f;
            f.open(data_path, std::ios::in | std::ios::binary);
            int32_t magic = nextint32(f);
            int32_t count = nextint32(f);
            int32_t rows = nextint32(f);
            int32_t cols = nextint32(f);
            data_ = torch::empty({count, rows, cols}, torch::kByte);
            f.read(reinterpret_cast<char*>(data_.data_ptr()), data_.numel());
            data_ = data_.to(torch::kFloat32).div_(255);
        }
    }

    torch::data::Example<> get(size_t idx) override {
        return { data_[idx], target_[idx] };
    }

    torch::optional<size_t> size() const override {
        return target_.size(0);
    }
 
private:
    int32_t nextint32(std::fstream& f) {
        int32_t i;
        f.read((char*)&i, sizeof(int32_t));
        i = __builtin_bswap32(i);
        return i;
    }

    uint8_t nextuint8(std::fstream& f) {
        uint8_t i;
        f.read((char*)&i, sizeof(uint8_t));
        i = __builtin_bswap32(i);
        return i;
    }


private:
    torch::Tensor data_;
    torch::Tensor target_;
};

struct Net : torch::nn::Module {
    Net() {
        fc1 = register_module("fc1", torch::nn::Linear(784, 64));
        fc2 = register_module("fc2", torch::nn::Linear(64, 32));
        fc3 = register_module("fc3", torch::nn::Linear(32, 10));
    }

    torch::Tensor forward(torch::Tensor x) {
        x = torch::relu(fc1->forward(x.reshape({x.size(0), 784})));
        x = torch::dropout(x, 0.5, is_training());
        x = torch::relu(fc2->forward(x));
        x = torch::log_softmax(fc3->forward(x), 1);
        return x;
    }

    torch::nn::Linear fc1{nullptr};
    torch::nn::Linear fc2{nullptr};
    torch::nn::Linear fc3{nullptr};
};

void train();

void test();

}
