#include "inference.h"

#ifdef ML

namespace wsldb {

int32_t nextint32(std::fstream& f) {
    int32_t i;
    f.read((char*)&i, sizeof(int32_t));
    i = __builtin_bswap32(i);
    return i;
}

uint8_t nextuint8(std::fstream& f) {
    uint8_t i;
    f.read((char*)&i, sizeof(uint8_t));
    return i;
}

Model::Model(torch::jit::script::Module model, 
             torch::jit::script::Module input_transform_fcn, 
             torch::jit::script::Module output_transform_fcn):
                    model_(model),
                    input_transform_fcn_(input_transform_fcn),
                    output_transform_fcn_(output_transform_fcn) {
                
    for (auto m: model_.named_modules()) {
        if (m.name == "") { //The first named parameters in the first module contains the input dimensions
            for (auto p: m.value.named_parameters()) {
                value_ = p.value;
                return;
            }
        }
    }
}

Status Model::Predict(const std::string& buf, std::vector<int>& results) {
    /*
    //TODO: need to add back in checks to make sure input/outputs match
    size_t type_size = 0;
    if (GetInputType() == torch::kByte) {
        type_size = sizeof(uint8_t);
    } else if (GetInputType() == torch::kFloat32) {
        type_size = sizeof(float);
    } else {
        return Status(false, "Inference Error: Invalid input type");
    }

    //if buf size and expected input size and type don't match, report error
    if (buf.size() % (type_size * GetInputSize()) != 0) {
        return Status(false, "Inference Error: Invalid input size");
    }

    int64_t image_count = buf.size() / (type_size * GetInputSize());

    torch::Tensor data = torch::empty({ image_count, GetInputSize() }, GetInputType());
    memcpy(data.data_ptr(), buf.data(), buf.size());

    return Status();*/

    //read buf to Tensor of kByte
    //change Tensor to std::vector<IValue>
    torch::Tensor data = torch::empty({ 1, int64_t(buf.size()) }, torch::kByte);
    memcpy(data.data_ptr(), buf.data(), buf.size());
    std::vector<torch::jit::IValue> input;
    input.push_back(data);

    torch::Tensor transformed_input = input_transform_fcn_(input).toTensor();
    std::vector<torch::jit::IValue> transformed_input_v;
    transformed_input_v.push_back(transformed_input);

    torch::Tensor model_output = model_.forward(transformed_input_v).toTensor();
    std::vector<torch::jit::IValue> model_output_v;
    model_output_v.push_back(model_output);

    torch::Tensor output = output_transform_fcn_(model_output_v).toTensor();

    for (size_t i = 0; i < output.size(0); i++) {
        results.push_back(output[i].item<int64_t>());
    }

    return Status();
}


Inference::Inference(const std::string& path): path_(path) {
}

Status Inference::GetModel(const std::string& name, Model** model) {
    if (models_.find(name) == models_.end()) {
        return Status(false, "Inference Error: Model not found");
    }

    *model = models_.at(name);
    return Status();
}

Status Inference::CreateModel(const std::string& name, const std::string& filename, const std::string& input_filename, const std::string&output_filename) {
    try {
        torch::jit::script::Module model_module = torch::jit::load(path_ + "/" + filename);
        torch::jit::script::Module input_module = torch::jit::load(path_ + "/" + input_filename);
        torch::jit::script::Module output_module = torch::jit::load(path_ + "/" + output_filename);
        Model* model =  new Model(std::move(model_module), std::move(input_module), std::move(output_module));
        models_.insert({name, model});
    } catch(const c10::Error& e) {
        return Status(false, "Inference Error: Error loading model");
    }

    return Status();
}

}

#endif
