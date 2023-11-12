// Este código lê um CSV e cria uma cópia deste com os dados categóricos
// trocados pelos seus respectivos ids além de um CSV para cada coluna
// categórica no formato (id|nome).

// Dataset link:
//     https://drive.google.com/file/d/1wfk_0QTIZA-uZktkOpwMmqFDVVIi5O-l/view?usp=sharing
//

#include <omp.h>
#include <stdio.h>
#include <stdlib.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>

std::string final_dataset_name = "final_dataset.csv";
// Dataset com os dados categóricos substituídos pelos seus respectivos ids
std::ofstream final_dataset;

// Nome das colunas categóricas
std::vector<std::string> category_names{
    "cdtup.csv",         "berco.csv",        "portoatracacao.csv",
    "mes.csv",           "tipooperacao.csv", "tiponavegacaoatracacao.csv",
    "terminal.csv",      "origem.csv",       "destino.csv",
    "naturezacarga.csv", "sentido.csv"};

// Índices das colunas categóricas
std::vector<int> category_indexes{1, 2, 3, 5, 6, 7, 8, 17, 18, 20, 23};

int categories_list_size = category_names.size();

// Dicionário de cada coluna categórica
std::map<std::string, std::vector<std::string>> categorical_dict;

bool started_writing_final_dataset = false;

void clean_existing_files();

void update_categorical_dict(std::string line);

void write_dict_files();

void write_final_dataset(std::string line);

int main(int argc, char* argv[]) {
  std::string line, filename;

  if (argc > 1) {
    auto start = std::chrono::high_resolution_clock::now();

    filename = argv[1];

    std::fstream dataset_to_read(filename, std::ios::in);

    if (dataset_to_read.is_open()) {
      clean_existing_files();

      while (getline(dataset_to_read, line)) {
        update_categorical_dict(line);
      }

      write_dict_files();

      line.clear();
      dataset_to_read.clear();
      dataset_to_read.seekg(0, dataset_to_read.beg);
      final_dataset.open(final_dataset_name);

      while (getline(dataset_to_read, line)) {
        write_final_dataset(line);
      }

      dataset_to_read.close();
      final_dataset.close();

      auto end = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

      std::cout << "Duration: " << duration.count() << "ms." << std::endl;
    } else {
      std::cout << "Could not open provided file name: \"" << filename << "\"."
                << std::endl;
      exit(1);
    }
  } else {
    std::cout << "Should execute along with the file name that will be read."
              << std::endl;
  }
}

void clean_existing_files() {
  for (auto category_name : category_names) {
    std::remove(category_name.c_str());
  }

  std::remove(final_dataset_name.c_str());
};

void update_categorical_dict(std::string line) {
  std::string content;
  std::stringstream line_stream(line);
  std::vector<std::string> row;

  int i = 0;

  while (getline(line_stream, content, ',')) {
    auto vec = category_indexes;

    if (std::find(vec.begin(), vec.end(), i) != vec.end()) {
      row.push_back(content);
    }

    i++;
  }

  for (int i = 0; i < categories_list_size; i++) {
    auto category_name = category_names[i];
    auto categorical_info = row[i];

    if (categorical_dict.find(category_name) == categorical_dict.end()) {
      std::vector<std::string> categorical_info_vector;
      categorical_info_vector.push_back(categorical_info);

      categorical_dict[category_name] = categorical_info_vector;
    } else {
      auto vec = categorical_dict[category_name];

      if (std::find(vec.begin(), vec.end(), categorical_info) == vec.end()) {
        categorical_dict[category_name].push_back(categorical_info);
      }
    }
  }
}

void write_dict_files() {
  for (int i = 0; i < categories_list_size; i++) {
    auto category_name = category_names[i];
    auto category_values = categorical_dict[category_name];
    auto category_list_size = category_values.size();

    std::ofstream my_file;

    my_file.open(category_name);

    my_file << "ID," << category_values[0] << std::endl;

    for (int j = 1; j < category_list_size; j++) {
      my_file << j << "," << category_values[j] << std::endl;
    }

    my_file.close();
  }
}

void write_final_dataset(std::string line) {
  if (final_dataset.is_open()) {
    std::string content;
    std::stringstream line_stream(line);

    int i = 0;

    while (getline(line_stream, content, ',')) {
      if (i != 0) {
        final_dataset << ",";
      }

      auto vec1 = category_indexes;
      auto raw_index = std::find(vec1.begin(), vec1.end(), i);

      if (started_writing_final_dataset && (raw_index != vec1.end())) {
        int index = std::distance(vec1.begin(), raw_index);
        auto category_name = category_names[index];

        auto vec2 = categorical_dict[category_name];
        auto raw_index = std::find(vec2.begin(), vec2.end(), content);
        int category_id = std::distance(vec2.begin(), raw_index);

        final_dataset << category_id;
      } else {
        final_dataset << content;
      }

      i++;
    }

    final_dataset << std::endl;

    if (!started_writing_final_dataset) {
      started_writing_final_dataset = true;
    }
  } else {
    std::cout << "Could not open provided file name: \"" << final_dataset_name
              << "\"." << std::endl;
    exit(1);
  }
}
