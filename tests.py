import os

for root, dirs, files in os.walk("./test_files/"):
    for file in files:
        if file == "reference":
            continue
        test_file = "./test_files/"+file
        print("#################"+test_file)
        os.system("python3 main.py "+test_file)
