// Este código lê um arquivo CSV e cria uma cópia deste arquivo com os dados
// categóricos trocados pelos seus respectivos ids além de um CSV para cada
// coluna categórica no formato (id|nome).

// Dataset link:
// https://drive.google.com/file/d/1wfk_0QTIZA-uZktkOpwMmqFDVVIi5O-l/view?usp=sharing
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

// Variável que armazena as informações lidas do arquivo no laço atual
std::vector<std::vector<std::string>> categorical_info_per_line;

// Variável que apontará para o arquivo a ser lido
std::fstream dataset_to_read;

// Constante que indica o número de linhas lidas por laço
int MAX_LINES_READ_PER_LOOP = 10000;

// Variável de controle que indica se a leitura do arquivo chegou ao final
bool concluded_reading_file = false;

// Nome do arquivo que conterá o dataset final
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

// Dicionário de cada coluna categórica
std::map<std::string, std::vector<std::string>> categorical_dict;

// Dicionário que conterá informações categóricas e seus id para
// checagem rápida se a informação lida do arquivo é categórica
std::map<std::string, std::string> categorical_info_map;

void clean_existing_files();

void initialize_dict();

void update_current_slice();

void update_categorical_dict();

void write_dict_files();

void write_final_dataset();

int main(int argc, char* argv[]) {
  std::string filename;

  if (argc > 1) {
    auto start = std::chrono::high_resolution_clock::now();

    filename = argv[1];

    dataset_to_read.open(filename, std::fstream::in);

    if (dataset_to_read.is_open()) {
      clean_existing_files();

      initialize_dict();

      while (!concluded_reading_file) {
        update_current_slice();

        update_categorical_dict();
      }

      write_dict_files();

      write_final_dataset();

      dataset_to_read.close();

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

  return 0;
}

void clean_existing_files() {
  for (auto category_name : category_names) {
    std::remove(category_name.c_str());
  }

  std::remove(final_dataset_name.c_str());
}

void initialize_dict() {
  for (auto category_name : category_names) {
    categorical_dict[category_name] = std::vector<std::string>();
  }
}

void update_current_slice() {
  categorical_info_per_line.clear();

  if (!concluded_reading_file) {
    std::string line;

    for (int i = 0; i < MAX_LINES_READ_PER_LOOP; i++) {
      if (getline(dataset_to_read, line)) {
        std::stringstream line_stream(line);
        std::string content;
        std::vector<std::string> row;

        int j = 0;

        while (getline(line_stream, content, ',')) {
          auto vec = category_indexes;

          if (std::find(vec.begin(), vec.end(), j) != vec.end()) {
            row.push_back(content);
          }

          j++;
        }

        categorical_info_per_line.push_back(row);
        line.clear();
      } else {
        concluded_reading_file = true;
        break;
      }
    }
  }
}

void update_categorical_dict() {
  int slice_size = categorical_info_per_line.size();
  int categories_num = category_names.size();

#pragma omp parallel
  {
    for (int i = 0; i < categories_num; i++) {
      auto category_name = category_names[i];
      std::vector<std::string> category_list;

#pragma omp for nowait
      for (int j = 0; j < slice_size; j++) {
        auto categorical_info = categorical_info_per_line[j][i];
        category_list.push_back(categorical_info);
      }
#pragma omp critical
      {
        categorical_dict[category_name].insert(
            categorical_dict[category_name].end(),
            std::make_move_iterator(category_list.begin()),
            std::make_move_iterator(category_list.end()));
      }
    }
  }

  for (std::string category_name : category_names) {
    std::sort(categorical_dict[category_name].begin(),
              categorical_dict[category_name].end());
    categorical_dict[category_name].erase(
        std::unique(categorical_dict[category_name].begin(),
                    categorical_dict[category_name].end()),
        categorical_dict[category_name].end());
  }
}

void write_dict_files() {
  for (auto category_info : categorical_dict) {
    auto category_file_name = category_info.first;
    auto category_values = category_info.second;

    std::fstream category_file;

    category_file.open(category_file_name, std::fstream::app);

    std::string category_name =
        category_file_name.substr(0, category_file_name.size() - 4);

    category_file << "ID," << category_name << std::endl;

    int category_id = 1;

    for (std::string categorical_info : category_values) {
      if (categorical_info != category_name) {
        categorical_info_map[categorical_info] = std::to_string(category_id);
        category_file << category_id << "," << categorical_info << std::endl;
        category_id++;
      }
    }
  }
}

void write_final_dataset() {
  dataset_to_read.clear();
  dataset_to_read.seekg(0, dataset_to_read.beg);
  concluded_reading_file = false;

  final_dataset.open(final_dataset_name);

  if (final_dataset.is_open()) {
    int i = 0;

    std::string line;

    while (getline(dataset_to_read, line)) {
      std::string raw_content;
      std::stringstream line_stream(line);

      int j = 0;

      while (getline(line_stream, raw_content, ',')) {
        if (j != 0) {
          final_dataset << ",";
        }

        std::string content =
            i > 0 && categorical_info_map.count(raw_content) > 0
                ? categorical_info_map[raw_content]
                : raw_content;

        final_dataset << content;

        j++;
      }

      final_dataset << std::endl;

      i++;
    }

    final_dataset.close();
  } else {
    std::cout << "Could not open provided file name: \"" << final_dataset_name
              << "\"." << std::endl;
    exit(1);
  }
}
