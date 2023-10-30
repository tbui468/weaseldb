#include "nn.h"
#include <memory>
#include <iostream>

namespace wsldb {

void train() {
    std::shared_ptr<Net> net = std::make_shared<Net>();

    auto data_loader = torch::data::make_data_loader(
            torch::data::datasets::MNIST("./../data").map(torch::data::transforms::Stack<>()), 
            /*batch_size=*/64);

    torch::optim::SGD optimizer(net->parameters(), /*lr=*/0.01);

    for (size_t epoch = 1; epoch <= 10; epoch++) {
        size_t batch_idx = 0;

        for (auto& batch: *data_loader) {
            optimizer.zero_grad();
            torch::Tensor prediction = net->forward(batch.data);
            torch::Tensor loss = torch::nll_loss(prediction, batch.target);
            loss.backward(); //compute gradients
            optimizer.step();

            if (++batch_idx % 100 == 0) {
                std::cout << "Epoch: " << epoch << " | Batch: " << batch_idx 
                          << " | Loss: " << loss.item<float>() << std::endl;
                torch::save(net, "net.pt");
            }
        }
    }
}

void test() {
    auto test_dataset = torch::data::datasets::MNIST("./../data", torch::data::datasets::MNIST::Mode::kTest).map(torch::data::transforms::Stack<>());
    const size_t dataset_size = test_dataset.size().value();
    auto data_loader = torch::data::make_data_loader(std::move(test_dataset), /*batch_size=*/ 64);

    std::shared_ptr<Net> model = std::make_shared<Net>();
    torch::load(model, "net.pt");
    model->eval();

    int32_t correct = 0;
    double test_loss = 0.0;

    for (const auto& batch: *data_loader) {
        auto output = model->forward(batch.data);
        test_loss += torch::nll_loss(
                            output,
                            batch.target,
                            {},
                            torch::Reduction::Sum)
                            .item<float>();
        auto pred = output.argmax(1);
        correct += pred.eq(batch.target).sum().item<int64_t>();
    }

    test_loss /= dataset_size;
    std::printf("\nTest set: Average loss: %.4f | Accuracy: %.3f\n",
            test_loss, static_cast<double>(correct)/dataset_size);
}

}
