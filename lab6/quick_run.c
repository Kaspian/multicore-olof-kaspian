echo 'Without OpenMP'
gcc -o mm mm.c -g -O3 -pthread -fgnu-tm
time ./mm
echo 'With OpenMP'
gcc -o mm mm_parallell.c -g -O3 -pthread -fgnu-tm -fopenmp
time ./mm
