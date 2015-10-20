#include <iostream>

int getSifts(float* image, int size, int width, int height);

int f() {
    int *x = (int*) malloc(100);
    return (long) x;
}
int main() {
    float image[1000*1000];
    std::generate_n(image, 1000*1000, std::rand);
    getSifts(image, 1000*1000, 1000, 1000);
}


