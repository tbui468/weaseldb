#pragma once

#include <torch/torch.h>
#include <torch/script.h>

#include <string>
#include <fstream>

#include "status.h"

namespace wsldb {

int32_t nextint32(std::fstream& f);
uint8_t nextuint8(std::fstream& f);

class Model {
public:
    Model(torch::jit::script::Module module);
    Status Predict(const std::string& buf, std::vector<int>& results);
    static Status ByteToFloat(const std::string& src, std::string& dst);
        
    inline caffe2::TypeMeta GetInputType() {
        return value_.dtype();
    }

    inline int GetInputSize() {
        return value_.size(1);
    }

private:
    torch::Tensor value_;
    torch::jit::script::Module module_;
};

class Inference {
public:
    Inference(const std::string& path);
    Status GetModel(const std::string& name, Model** model);
    Status CreateModel(const std::string& name, const std::string& filename);
    std::vector<int> Predict(); //TODO: debug function
private:
    std::string path_;
    std::unordered_map<std::string, Model*> models_;
    std::string buf_;
};

}
