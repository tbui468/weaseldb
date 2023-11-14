#pragma once

#ifdef ML

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
    Model(torch::jit::script::Module model, 
          torch::jit::script::Module input_transform_fcn, 
          torch::jit::script::Module output_transform_fcn);

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
    torch::jit::script::Module model_;
    torch::jit::script::Module input_transform_fcn_;
    torch::jit::script::Module output_transform_fcn_;
};

class Inference {
public:
    Inference(const std::string& path);
    Status GetModel(const std::string& name, Model** model);
    Status CreateModel(const std::string& name, const std::string& filename, const std::string& input_filename, const std::string&output_filename);
private:
    std::string path_;
    std::unordered_map<std::string, Model*> models_;
    std::string buf_;
};


}

#else

#include <string>
#include <vector>
#include "status.h"

//This is empty functions for when compiled without pytorch
namespace wsldb {
class Model {
public:
    Status Predict(const std::string& buf, std::vector<int>& results) { return Status(); }
};

class Inference {
public:
    Inference(const std::string& path) {}
    Status GetModel(const std::string& name, Model** model) { return Status(); }
    Status CreateModel(const std::string& name, const std::string& filename) { return Status(); }
    std::vector<int> Predict() { return {}; }
};
}

#endif
