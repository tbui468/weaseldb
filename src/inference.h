#pragma once

#ifdef ML

#include <torch/torch.h>
#include <torch/script.h>

#include <string>
#include <fstream>
#include <vector>

#include "status.h"

namespace wsldb {

class Model {
public:
    Model(torch::jit::script::Module model);
    Status Predict(const std::string& buf, std::vector<int>& results);
        
    inline caffe2::TypeMeta GetInputType() {
        return value_.dtype();
    }

    inline int GetInputSize() {
        return value_.size(1);
    }

private:
    //caching value_ so that input types and sizes can be computed
    torch::Tensor value_;
    torch::jit::script::Module model_;
};

class Inference {
public:
    Inference(const std::string& path): path_(path) {}
    Status DeserializeModel(const std::string& deserialized_model, Model** model);
    inline std::string CreateFullModelPath(const std::string& filename) const { return path_ + "/" + filename; }
private:
    std::string path_;
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
    Status DeserializeModel(const std::string& serialized_model, Model** model) { return Status(); }
    inline std::string CreateFullModelPath(const std::string& filename) const { return ""; }
};
}

#endif
