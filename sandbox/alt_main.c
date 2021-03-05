// alt_sandbox is basically the same as sandbox but doesn't restrict networking.
#include <stdio.h>
#include "sandbox/sandbox.h"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fputs("alt_sandbox implements limited sandboxing via Linux namespaces.\n", stderr);
        fputs("It takes no flags, it simply executes the command given as arguments.\n", stderr);
        fputs("Usage: alt_sandbox command args...\n", stderr);
        return 1;
    }
    return contain(&argv[1], false, true);
}
