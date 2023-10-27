# path/to/your/file.py

import argparse

def main():
    parser = argparse.ArgumentParser(description='Processa um arquivo modificado na pasta "resource".')
    parser.add_argument('filename', help='Nome do arquivo modificado na pasta "resource".')

    args = parser.parse_args()

    print(f'Arquivo modificado na pasta "resource": {args.filename}')

if __name__ == "__main__":
    main()