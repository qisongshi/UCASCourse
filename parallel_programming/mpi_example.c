#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "mpi.h"

// 可追加头文件
// 读取和写入的代码可放入指定的进程中进行

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/time.h>
#endif

#include <time.h>

static double timestamp;

#ifdef _WIN32
int gettimeofday(struct timeval *tp, void *tzp)
{
    time_t clock;
    struct tm tm;
    SYSTEMTIME wtm;
    GetLocalTime(&wtm);
    tm.tm_year = wtm.wYear - 1900;
    tm.tm_mon = wtm.wMonth - 1;
    tm.tm_mday = wtm.wDay;
    tm.tm_hour = wtm.wHour;
    tm.tm_min = wtm.wMinute;
    tm.tm_sec = wtm.wSecond;
    tm.tm_isdst = -1;
    clock = mktime(&tm);
    tp->tv_sec = clock;
    tp->tv_usec = wtm.wMilliseconds * 1000;
    return (0);
}
#endif

double get_time()
{
    struct timeval tp;
    gettimeofday(&tp, NULL);
    return ((double)tp.tv_sec + (double)tp.tv_usec * 1e-6);
}

void start_perf()
{
    timestamp = get_time();
}

void end_perf(const char *str)
{
    timestamp = get_time() - timestamp;
    printf("PERF: %s times cost: %.6lfs\n", str, timestamp);
}

int main(int argc, char **argv)
{
    int rows = 6400;
    int cols = 6400;

    // 为矩阵分配内存
    int32_t *matrix = (int32_t *)malloc(rows * cols * sizeof(int32_t));
    int32_t *zz_matrix = (int32_t *)malloc(rows * cols * sizeof(int32_t));
    int32_t *tmp_matrix = (int32_t *)malloc(rows * cols / 2 * sizeof(int32_t));
    int32_t *zz_tmp_matrix = (int32_t *)malloc(rows * cols / 2 * sizeof(int32_t));
    int32_t *result_matrix = (int32_t *)malloc(rows / 2 * cols / 2 * sizeof(int32_t));

    // 从二进制文件中读取矩阵数据
    FILE *file = fopen("random_matrix.bin", "rb");
    if (file != NULL) {
        fread(matrix, sizeof(int32_t), rows * cols, file);
        fclose(file);
        printf("Matrix has been read from random_matrix.bin\n");

    } else {
        printf("Error opening file for reading\n");
    }

    int32_t *phase1_matrix_1, *phase1_matrix_2, *phase2_matrix_1, *phase2_matrix_2;
    start_perf();
    /*******************************************/
    // TODO: 在这里实现openMP或MPI的程序逻辑，以matrix作为输入，以result_matrix作为输出
    /*******************************************/

    // 先做矩阵转置
    for (int i = 0; i < rows; i++)
    {
        for (int j = 0; j < cols; j++)
        {
            zz_matrix[j * cols + i] = matrix[i * cols + j];
        }
    }

    int cf = 8;
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    phase1_matrix_1 = (int32_t *)malloc(rows * cols / cf * sizeof(int32_t));
    phase1_matrix_2 = (int32_t *)malloc(rows * cols / cf / 2 * sizeof(int32_t));
    phase2_matrix_1 = (int32_t *)malloc(rows * cols / cf / 2 * sizeof(int32_t));
    phase2_matrix_2 = (int32_t *)malloc(rows * cols / cf / 4 * sizeof(int32_t));

    MPI_Scatter(zz_matrix, rows * cols / cf, MPI_INT, phase1_matrix_1, rows * cols / cf, MPI_INT, 0, MPI_COMM_WORLD);

    for (int i = 0; i < rows * cols / cf / 2; i++)
        phase1_matrix_2[i] = phase1_matrix_1[i] + phase1_matrix_1[i + rows * cols / cf / 2];

    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Gather(phase1_matrix_2, rows * cols / cf / 2, MPI_INT, zz_tmp_matrix, rows * cols / cf / 2, MPI_INT, 0, MPI_COMM_WORLD);

    // 再做矩阵转置
    if (rank == 0)
    {
        for (int i = 0; i < rows / 2; i++)
            for (int j = 0; j < cols; j++)
                tmp_matrix[j * cols / 2 + i] = zz_tmp_matrix[i * cols + j];
    }

    MPI_Scatter(tmp_matrix, rows * cols / cf / 2, MPI_INT, phase2_matrix_1, rows * cols / cf / 2, MPI_INT, 0, MPI_COMM_WORLD);

    for (int i = 0; i < rows * cols / cf / 4; i++)
        phase2_matrix_2[i] = phase2_matrix_1[i] + phase2_matrix_1[i + rows * cols / cf / 4];

    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Gather(phase2_matrix_2, rows * cols / cf / 4, MPI_INT, result_matrix, rows * cols / cf / 4, MPI_INT, 0, MPI_COMM_WORLD);

    free(phase1_matrix_1);
    free(phase1_matrix_2);
    free(phase2_matrix_1);
    free(phase2_matrix_2);

    MPI_Finalize();

    end_perf("MPI");

    // 将矩阵写入二进制文件
    file = fopen("result_matrix.bin", "wb");
    if (file != NULL) {
        fwrite(result_matrix, sizeof(int32_t), rows / 2 * cols / 2, file);
        fclose(file);
        printf("Matrix has been written to result_matrix.bin\n");
    } else {
        printf("Error opening file for writing\n");
    }

    // 释放内存
    free(matrix);
    free(zz_matrix);
    free(tmp_matrix);
    free(zz_tmp_matrix);
    free(result_matrix);

    return 0;
}