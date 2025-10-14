echo 'Without OpenMP'
gcc -o mm mm.c -g -O3 -pthread -fgnu-tm
time ./mm
echo 'With OpenMP'
gcc -o mm mm_parallell.c -g -O3 -pthread -fgnu-tm -fopenmp
time ./mm
echo 'Clang'
clang -o mm -mcpu=power8 -O3 mm.c
time ./mm
echo 'FExp'
gcc -o mm -fexpensive-optimizations -mcpu=power8 -O3 mm.c
time ./mm
echo 'FTree-Parallelize'
gcc -o mm -ftree-parallelize-loops=80 -fexpensive-optimizations -mcpu=power8 -O3 mm.c
time ./mm
echo 'PGCC'
pgcc -o mm -tp=pwr8 -O4 mm.c -Mconcur=allcores
time ./mm
echo 'XLC'
xlc -o mm -qarch=pwr8 -O5 -qsmp -qhot=level=2 mm.c
time ./mm
