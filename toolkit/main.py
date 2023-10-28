import argparse

def main():
    # Argument 'filename' configuration
    parser = argparse.ArgumentParser(description='Process modified files.')
    parser.add_argument('--filename', type=str, help='Name of the modified file')
    args = parser.parse_args()

    # Check if the 'filename' argument is provided
    if args.filename:
        print(f'Processing file: {args.filename}')
        # Do whatever you need with the file, for example:
        # with open(args.filename, 'r') as file:
        #     data = file.read()
        #     # Perform the necessary processing

    else:
        print("No filename provided.")

if __name__ == '__main__':
    main()