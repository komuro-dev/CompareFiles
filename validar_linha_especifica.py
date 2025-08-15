#!/usr/bin/env python3
"""
Validador de Linhas Específicas (Versão Autônoma)
Autor: Sistema de Comparação
Data: 2024
"""

import os
import sys
import hashlib
from typing import List, Optional, Any

# ==============================================================================
# ### CONFIGURAÇÕES ###
# Altere os valores abaixo para apontar para os seus arquivos e definir o
# delimitador correto.
# ==============================================================================

# Use r'...' para evitar problemas com barras invertidas no Windows
ARQUIVO_1_PATH = r'C:\EngDb\Eneva\Code\CompareFiles\txt_SAP\ADRC_20250725_115350.txt'
ARQUIVO_2_PATH = r'C:\EngDb\Eneva\Code\CompareFiles\txt_BQ\20250808\adrc_20250808_193426.txt'

# Delimitador usado nos arquivos CSV/TXT
DELIMITADOR = '!@#'

# Defina como True para normalizar números (ex: "1.000,50" -> 1000.50) e remover espaços
# Defina como False para comparar os dados exatamente como estão no arquivo
NORMALIZAR_DADOS = True


# ==============================================================================
# ### FIM DAS CONFIGURAÇÕES ###
# Nenhuma alteração é necessária abaixo desta linha.
# ==============================================================================


def create_row_hash(row_data: List) -> str:
    """
    Cria um hash MD5 único para uma lista de valores de uma linha.
    """
    row_str = '|'.join(str(val) for val in row_data)
    return hashlib.md5(row_str.encode()).hexdigest()


def _normalize_numeric_string(value: Any) -> Any:
    """
    Normaliza uma string que pode representar um número.
    """
    if isinstance(value, str):
        original_value = value
        value = value.replace(' ', '')
        num_dots = value.count('.')
        num_commas = value.count(',')

        if num_commas > 0 and num_dots > 0:
            last_dot_idx = value.rfind('.')
            last_comma_idx = value.rfind(',')
            if last_comma_idx > last_dot_idx:
                value = value.replace('.', '')
                value = value.replace(',', '.')
            elif last_dot_idx > last_comma_idx:
                value = value.replace(',', '')
        elif num_commas > 0:
            if value.count(',') == 1:
                value = value.replace(',', '.')
            else:
                value = value.replace(',', '')

        try:
            return float(value)
        except ValueError:
            return original_value.strip()
    return value


def process_line(line_content: str, delimiter: str, normalize: bool) -> List[Any]:
    """
    Processa uma linha de texto: divide, limpa e, opcionalmente, normaliza.
    """
    cleaned_line = line_content.strip()
    fields = [field.strip() for field in cleaned_line.split(delimiter)]

    if normalize:
        return [_normalize_numeric_string(field) for field in fields]

    return fields


def get_line_from_file(file_path: str, line_number: int) -> Optional[str]:
    """
    Lê uma linha específica de um arquivo de forma eficiente.
    """
    target_line_idx = line_number - 1

    if target_line_idx < 0:
        print(f"❌ Número da linha inválido: {line_number}. Deve ser 1 ou maior.")
        return None

    if not os.path.exists(file_path):
        print(f"❌ ERRO FATAL: Arquivo de configuração não encontrado em '{file_path}'")
        print("   Por favor, verifique a variável ARQUIVO_1_PATH ou ARQUIVO_2_PATH no topo do script.")
        sys.exit(1)  # Encerra o programa se o arquivo não existe

    encodings_to_try = ['utf-8', 'latin1', 'cp1252']

    for encoding in encodings_to_try:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                for i, line in enumerate(f):
                    if i == target_line_idx:
                        return line
                print(
                    f"❌ Erro: Linha {line_number} não encontrada em '{os.path.basename(file_path)}'. O arquivo tem menos linhas.")
                return None
        except UnicodeDecodeError:
            continue

    print(f"❌ Erro: Não foi possível ler o arquivo '{os.path.basename(file_path)}' com os encodings testados.")
    return None


def main():
    """Função principal para validar linhas específicas."""
    print("🔎 VALIDADOR DE LINHA ESPECÍFICA E HASH MD5 (VERSÃO AUTÔNOMA)")
    print("=" * 60)

    # Usa as variáveis de configuração definidas no topo do script
    file1 = ARQUIVO_1_PATH
    file2 = ARQUIVO_2_PATH
    delimiter = DELIMITADOR
    normalize = NORMALIZAR_DADOS

    print(f"📁 Arquivo 1: {os.path.basename(file1)}")
    print(f"📁 Arquivo 2: {os.path.basename(file2)}")
    print(f"🔧 Delimitador: '{delimiter}'")
    print(f"✨ Normalização de dados: {'Ativada' if normalize else 'Desativada'}")
    print("=" * 60)

    while True:
        try:
            pos1_str = input("Digite a posição da linha no Arquivo 1 (ou 'sair' para encerrar): ")
            if pos1_str.lower() in ['sair', 'exit', 'quit', 's']:
                break
            pos1 = int(pos1_str)

            pos2_str = input("Digite a posição da linha no Arquivo 2 (ou 'sair' para encerrar): ")
            if pos2_str.lower() in ['sair', 'exit', 'quit', 's']:
                break
            pos2 = int(pos2_str)

            print("\n" + "-" * 20 + " ANÁLISE " + "-" * 20)

            line1_content = get_line_from_file(file1, pos1)
            line2_content = get_line_from_file(file2, pos2)

            if line1_content and line2_content:
                processed_line1 = process_line(line1_content, delimiter, normalize)
                processed_line2 = process_line(line2_content, delimiter, normalize)

                hash1 = create_row_hash(processed_line1)
                hash2 = create_row_hash(processed_line2)

                print(f"\n[ARQUIVO 1 - Linha {pos1}]")
                print(f"  📝 Conteúdo Original: {line1_content.strip()}")
                print(f"  ✨ Valores Processados: {processed_line1}")
                print(f"  🔑 Hash MD5: {hash1}")

                print(f"\n[ARQUIVO 2 - Linha {pos2}]")
                print(f"  📝 Conteúdo Original: {line2_content.strip()}")
                print(f"  ✨ Valores Processados: {processed_line2}")
                print(f"  🔑 Hash MD5: {hash2}")

                print("\n" + "-" * 18 + " RESULTADO " + "-" * 19)
                if hash1 == hash2:
                    print("✅ VEREDITO: Os hashes SÃO IDÊNTICOS. As linhas contêm os mesmos dados.")
                else:
                    print("❌ VEREDITO: Os hashes SÃO DIFERENTES. As linhas não contêm os mesmos dados.")
                print("-" * 50 + "\n")
            else:
                print("\n⚠️  Não foi possível realizar a análise pois uma ou ambas as linhas não foram lidas.\n")

        except ValueError:
            print("\n❌ Entrada inválida. Por favor, digite um número inteiro para a posição da linha.\n")
        except Exception as e:
            print(f"\n❌ Ocorreu um erro inesperado: {e}\n")

    print("✅ Programa encerrado.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Operação cancelada pelo usuário. Encerrando.")
        sys.exit(0)