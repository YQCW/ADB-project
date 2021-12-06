import argparse
from TransactionManager import TransactionManager
from DataManager import site
from Operation import *

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("inputFile", type=str, help="input file")
    args = parser.parse_args()
    inputFile = args.inputFile

    #initialize the transaction manager and sites
    tm = TransactionManager()
    for i in range(1,11):
        tm.sites.append(site(i))

    with open(inputFile) as input:
        time = 0
        for line in inputFile:
            time += 1
            type, parameters = parseOp(line)
            operation = None
            if type == "begin":
                operation = Begin(parameters)
            elif type == "beginRO":
                operation = BeginRO(parameters)
            elif type == "end":
                operation = End(parameters)
            elif type == "W":
                operation = W(parameters)
            elif type == "R":
                operation = R(parameters)
            elif type == "dump":
                operation = Dump(parameters)
            elif type == "fail":
                operation = Fail(parameters)
            elif type == "recover":
                operation = Recover(parameters)
            else:
                print("error operation type!!!!!")
                exit(1)
            tm.step(operation, time)