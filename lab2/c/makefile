main:
	gcc -o preflow preflow.c -g -O3 -pthread
	time sh check-solution.sh ./preflow
	@echo PASS all tests

helgrind:
	gcc -o preflow preflow.c -g -O0 -pthread
	time valgrind --tool=helgrind sh check-solution.sh ./preflow
