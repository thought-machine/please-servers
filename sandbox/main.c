// This is a copy of Please's sandbox to put it into a form that is more convenient for us
// to have a 'sandbox' and 'alt_sandbox'.
// Later on we'll probably upstream this.
#include <stdio.h>
#include "sandbox/sandbox.h"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fputs("sandbox implements sandboxing for Please.\n", stderr);
        fputs("It takes no flags, it simply executes the command given as arguments.\n", stderr);
        fputs("Usage: sandbox command args...\n", stderr);
        return 1;
    }
    return contain(&argv[1], true, true);
}
