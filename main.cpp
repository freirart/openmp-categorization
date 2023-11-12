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

void clean_existing_files();

void update_categorical_dict(std::string line);

void write_dict_files();

int main(int argc, char* argv[]) {
  std::string line, content;

  if (argc > 1) {
    auto start = std::chrono::high_resolution_clock::now();

    std::fstream dataset_to_read(argv[1], std::ios::in);

    if (dataset_to_read.is_open()) {
      clean_existing_files();

      while (getline(dataset_to_read, line)) {
        update_categorical_dict(line);
      }

      write_dict_files();

      dataset_to_read.close();

      auto end = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

      std::cout << "Duration: " << duration.count() << "ms." << std::endl;
    } else {
      std::cout << "Could not open provided file name." << std::endl;
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
