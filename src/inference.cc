#include "inference.h"

#ifdef ML

namespace wsldb {

Model::Model(torch::jit::script::Module model): model_(model) {
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

    /*
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

    return Status();*/

    torch::Tensor data = torch::empty({ int64_t(buf.size()) }, torch::kByte);
    memcpy(data.data_ptr(), buf.data(), buf.size());

    torch::Tensor output = model_.run_method("wsldb_input", data).toTensor();
    output = model_.run_method("forward", output).toTensor();
    output = model_.run_method("wsldb_output", output).toTensor();

    for (int i = 0; i < output.size(0); i++) {
        results.push_back(output[i].item<int64_t>());
    }

    return Status();
}

Status Inference::DeserializeModel(const std::string& serialized_model, Model** model) {
    try {
        std::stringstream ss(serialized_model);
        torch::jit::script::Module module = torch::jit::load(ss);
        *model = new Model(std::move(module));
        return Status();
    } catch(const c10::Error& e) {
        return Status(false, "Inference Error: Error loading model");
    }
}

}

#endif
