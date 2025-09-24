main:
	gcc -o preflow preflow.c pthread_barrier.c -g -O3 -pthread
	time sh check-solution.sh ./preflow
	@echo PASS all tests

no_optimization:
	gcc -o preflow preflow.c pthread_barrier.c -g -O0 -pthread
	time sh check-solution.sh ./preflow
	@echo PASS all tests


