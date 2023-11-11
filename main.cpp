// Este código lê um CSV e cria uma cópia deste com os dados categóricos
// trocados pelos seus respectivos ids além de um CSV para cada coluna
// categórica no formato (id|nome).
//
// Funcionamento:
//     1.(sequencial) lê o dataset e armazena em uma matriz de X linhas e 26
//         colunas
//     2.(MultiThread) distribui as threads para que elas:
//         -> criem os arquivos coluna.csv
//         -> leem a matriz, verificam se o valor lido está no arquivo (SE NÃO
//             ESTIVER insere o valor no arquivo) e troca a string da matriz
//             para o id
//         -> esperam as outras threads
//     3.(sequencial) escreve o arquivo final
//
// Recursos criados:
//     var de nome das colunas
//     dicionário com o valor das index da coluna dos dados categóricos
//
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

// Dicionário que mapeia os dados categóricos e seus respectivos índices
std::map<std::string, int> category_index_map = {
    {"cdtup.csv", 1},          {"berco.csv", 2},
    {"portoatracacao.csv", 3}, {"mes.csv", 5},
    {"tipooperacao.csv", 6},   {"tiponavegacaoatracacao.csv", 7},
    {"terminal.csv", 8},       {"origem.csv", 17},
    {"destino.csv", 18},       {"naturezacarga.csv", 20},
    {"sentido.csv", 23}};

// Nome das colunas categóricas
std::vector<std::string> category_names;

// Índices das colunas categóricas
std::vector<int> category_indexes;

// Dicionário de cada coluna categórica
std::map<std::string, std::vector<std::string>> categorical_dict;

void initialize_category_info();

void clean_existing_files();

void update_categorical_dict(std::string line);

void dev_display_dict();

int main(int argc, char* argv[]) {
  std::string line, content;

  if (argc > 1) {
    auto start = std::chrono::high_resolution_clock::now();

    std::fstream dataset_to_read(argv[1], std::ios::in);

    if (dataset_to_read.is_open()) {
      initialize_category_info();

      clean_existing_files();

      while (getline(dataset_to_read, line)) {
        update_categorical_dict(line);
      }

      dev_display_dict();

      dataset_to_read.close();

      auto end = std::chrono::high_resolution_clock::now();
      std::chrono::duration<double, std::milli> ms_double = end - start;
      std::cout << "Duration: " << ms_double.count() << "ms." << std::endl;
    } else {
      std::cout << "Could not open provided file name." << std::endl;
    }
  } else {
    std::cout << "Should execute along with the file name that will be read."
              << std::endl;
  }
}

void initialize_category_info() {
  for (auto&& category_info : category_index_map) {
    category_names.push_back(category_info.first);
    category_indexes.push_back(category_info.second);
  }
}

void clean_existing_files() {
  for (std::string category_name : category_names) {
    std::remove(category_name.c_str());
  }
};

void update_categorical_dict(std::string line) {
  std::string word;
  std::stringstream line_stream(line);
  std::vector<std::string> row;

  int i = 0;

  while (getline(line_stream, word, ',')) {
    auto vec = category_indexes;

    if (std::find(vec.begin(), vec.end(), i) != vec.end()) {
      row.push_back(word);
    }

    i++;
  }

  int name_list_size = category_names.size();

#pragma omp parallel for
  for (int i = 0; i < name_list_size; i++) {
    std::string category_name = category_names[i];
    std::string categorical_info = row[i];

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

void dev_display_dict() {
  for (auto&& category_info : categorical_dict) {
    std::cout << "> Exibindo informações da coluna \"" << category_info.first
              << "\":" << std::endl;
    for (std::string categorical_info : category_info.second) {
      std::cout << categorical_info << std::endl;
    }
  }
}
