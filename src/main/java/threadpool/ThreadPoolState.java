package threadpool;

enum ThreadPoolState {
    STARTED,
    SHUT_DOWN_WITHOUT_INTERRUPTS,
    SHUT_DOWN_WITH_INTERRUPTS,
    TERMINATED
}
