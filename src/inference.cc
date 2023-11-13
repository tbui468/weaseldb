#include "inference.h"

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

Model::Model(torch::jit::script::Module module): module_(std::move(module)) {
    for (auto m: module_.named_modules()) {
        if (m.name == "") { //The first named parameters in the first module contains the input dimensions
            for (auto p: m.value.named_parameters()) {
                value_ = p.value;
                return;
            }
        }
    }
}

Status Model::Predict(const std::string& buf, std::vector<int>& results) {
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
    std::vector<torch::jit::IValue> inputs;
    inputs.push_back(data);
    at::Tensor output = module_.forward(inputs).toTensor();

    for (size_t i = 0; i < output.size(0); i++) {
        results.push_back(output.argmax(1)[i].item<int64_t>());
    }

    return Status();
}

Status Model::ByteToFloat(const std::string& src, std::string& dst) {
    for (size_t i = 0; i < src.size(); i++) {
        float f = float(uint8_t(src[i])) / 255.f;
        dst.append((char*)&f, sizeof(float));
    }

    return Status();
}

Inference::Inference(const std::string& path): path_(path) {
    /*
    //TODO: Later, need to deserialize the model that is serialized directly in the database

    //TODO: Debug loading data for mnist model prediction
    {
        const char* data_path = "./../../pytorch_to_cpp/old_data/t10k-images-idx3-ubyte";
        std::fstream f;
        f.open(data_path, std::ios::in | std::ios::binary);
        nextint32(f); //magic number
        int32_t count = nextint32(f);
        int32_t rows = nextint32(f);
        int32_t cols = nextint32(f);

        for (int i = 0; i < count * rows * cols; i++) {
            float fl = float(nextuint8(f)) / 255.f;
            buf_.append((char*)&fl, sizeof(float));
        }
    }

    //TODO: temp model
    CreateModel("my_model", path_ + "/traced_mnist_model.pt"); */
}

Status Inference::GetModel(const std::string& name, Model** model) {
    if (models_.find(name) == models_.end()) {
        return Status(false, "Inference Error: Model not found");
    }

    *model = models_.at(name);
    return Status();
}

Status Inference::CreateModel(const std::string& name, const std::string& filename) {
    try {
        torch::jit::script::Module module = torch::jit::load(path_ + "/" + filename);
        Model* model =  new Model(std::move(module));
        models_.insert({name, model});
    } catch(const c10::Error& e) {
        return Status(false, "Inference Error: Error loading model");
    }

    return Status();
}
std::vector<int> Inference::Predict() {
    std::vector<int> results;
    Status status = models_["my_model"]->Predict(buf_, results); //returns error if model input size/type don't align with size of data buffer
    return results;
}

}
