#include <omp.h>
#include <stdio.h>

int main() {
  printf("> Máximo de threads: %d.\n", omp_get_max_threads());
#pragma omp parallel
  {
    printf("> Número de threads utilizados na região paralela: %d.\n",
           omp_get_num_threads());
  }
}
