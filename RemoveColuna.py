import os


def remover_coluna_interativo(caminho_arquivo_entrada: str, delimitador: str = ','):
    """
    Analisa um arquivo, permite ao usuário escolher uma coluna para remover
    e salva o resultado em um novo arquivo com o sufixo '_R'.

    Args:
        caminho_arquivo_entrada (str): O caminho para o arquivo de entrada.
        delimitador (str): O caractere que separa as colunas (ex: ',', ';', '\\t').
                           O padrão é a vírgula.
    """
    # 1. Verifica se o arquivo de entrada realmente existe
    if not os.path.exists(caminho_arquivo_entrada):
        print(f"Erro: O arquivo '{caminho_arquivo_entrada}' não foi encontrado.")
        return

    primeiras_linhas = []
    num_colunas = 0

    print("--- Analisando o arquivo... ---")
    try:
        with open(caminho_arquivo_entrada, 'r', encoding='utf-8') as f:
            # 2. Lê as primeiras 5 linhas para visualização
            for i, linha in enumerate(f):
                if i < 5:
                    primeiras_linhas.append(linha.strip())
                # Determina o número de colunas baseado na primeira linha
                if i == 0:
                    num_colunas = len(linha.split(delimitador))

    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo: {e}")
        return

    if not primeiras_linhas:
        print("O arquivo parece estar vazio. Saindo.")
        return

    print("\n--- Visualização das 5 primeiras linhas ---")
    for linha_preview in primeiras_linhas:
        print(linha_preview)
    print("-------------------------------------------\n")

    # 3. Informa os números de colunas disponíveis
    print(f"O arquivo possui {num_colunas} colunas.")
    indices_disponiveis = list(range(num_colunas))
    print(f"Colunas disponíveis (por número): {indices_disponiveis}")

    coluna_para_remover = -1
    while True:
        # 4. Pergunta qual coluna remover
        try:
            entrada = input("\n> Digite o NÚMERO da coluna que você deseja remover: ")
            coluna_para_remover = int(entrada)
            if coluna_para_remover in indices_disponiveis:
                break  # Entrada válida, sai do loop
            else:
                print(
                    f"Erro: Número inválido. Por favor, escolha um número entre {indices_disponiveis[0]} e {indices_disponiveis[-1]}.")
        except ValueError:
            print("Erro: Entrada inválida. Por favor, digite um número.")

    print(f"\nOK. Removendo a coluna {coluna_para_remover}...")

    # Constrói o nome do arquivo de saída
    base, extensao = os.path.splitext(caminho_arquivo_entrada)
    caminho_arquivo_saida = f"{base}_R{extensao}"

    linhas_processadas = 0

    try:
        # 5. Abre os arquivos para processamento linha a linha
        with open(caminho_arquivo_entrada, 'r', encoding='utf-8') as f_entrada, \
                open(caminho_arquivo_saida, 'w', encoding='utf-8') as f_saida:

            for linha in f_entrada:
                # Remove quebra de linha no final
                linha_limpa = linha.strip()
                colunas = linha_limpa.split(delimitador)

                # 6. Remove a coluna especificada
                if len(colunas) > coluna_para_remover:
                    colunas.pop(coluna_para_remover)

                # 7. Junta as colunas restantes com o delimitador e escreve no novo arquivo
                nova_linha = delimitador.join(colunas)
                f_saida.write(nova_linha + '\n')
                linhas_processadas += 1

        print("\n--- Processo Concluído ---")
        print(f"Arquivo de saída criado em: {caminho_arquivo_saida}")
        print(f"Coluna {coluna_para_remover} foi removida de {linhas_processadas} linhas.")

    except Exception as e:
        print(f"Ocorreu um erro durante o processamento: {e}")


# --- Exemplo de como usar o script ---
# Coloque aqui o nome do seu arquivo.
# Pode ser um .csv, .txt, .log, etc.
nome_do_arquivo = r"C:\EngDb\Eneva\Code\CompareFiles\txt_BQ\20250804\hml-g1\Hml_lfa1_20250805_124710_U.txt"

# IMPORTANTE: Se o seu arquivo não usa vírgula (,) como separador,
# altere o valor abaixo.
# Para arquivos separados por ponto-e-vírgula: ';'
# Para arquivos separados por tabulação: '\t'
separador_de_coluna = '!@#'

remover_coluna_interativo(nome_do_arquivo, delimitador=separador_de_coluna)