#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "omp.h"

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

int main()
{
    int rows = 6400;
    int cols = 6400;

    // 为矩阵分配内存
    int32_t *matrix = (int32_t *)malloc(rows * cols * sizeof(int32_t));
    int32_t *result_matrix = (int32_t *)malloc(rows / 2 * cols / 2 * sizeof(int32_t));

    // 从二进制文件中读取矩阵数据
    FILE *file = fopen("random_matrix.bin", "rb");
    if (file != NULL)
    {
        fread(matrix, sizeof(int32_t), rows * cols, file);
        fclose(file);
        printf("Matrix has been read from random_matrix.bin\n");
    }
    else
    {
        printf("Error opening file for reading\n");
    }

    start_perf();

    int32_t *tmp_matrix = (int32_t *)malloc(rows * cols * sizeof(int32_t));
    omp_set_num_threads(8);
    int cf = 8;

    #pragma omp for
    for(int i = 0; i < rows; i++)
    {
        for(int j = 0; j < cf; j++)
        {
            for(int k = 0; k < (cols / cf / 2); k++)
                tmp_matrix[i * cols / 2 + j * (cols / cf / 2) + k] = matrix[i * cols + j * (cols / cf) + k] + matrix[i * cols + j * (cols / cf) + k + (cols / cf / 2)];
        }
    }

    #pragma omp barrier

    #pragma omp for
    for(int i = 0; i < cf; i++)
    {
        for(int j = 0; j < (rows / cf / 2); j++)
        {
            for(int k = 0; k < (cols / 2); k++)
                result_matrix[(j + i * rows / cf / 2) * cols / 2 + k] = tmp_matrix[(j + i * rows / cf) * cols / 2 + k] + tmp_matrix[(j + i * rows / cf + rows / cf / 2) * cols / 2 + k];
        }
    }

    end_perf("openMP");

    // 将矩阵写入二进制文件
    file = fopen("result_matrix.bin", "wb");
    if (file != NULL)
    {
        fwrite(result_matrix, sizeof(int32_t), rows / 2 * cols / 2, file);
        fclose(file);
        printf("Matrix has been written to result_matrix.bin\n");
    }
    else
    {
        printf("Error opening file for writing\n");
    }
    // 释放内存
    free(matrix);
    free(tmp_matrix);
    free(result_matrix);

    return 0;
}