import os

def remover_duplicatas(caminho_arquivo_entrada: str):

    # Verifica se o arquivo de entrada realmente existe
    if not os.path.exists(caminho_arquivo_entrada):
        print(f"Erro: O arquivo '{caminho_arquivo_entrada}' não foi encontrado.")
        return

    # Constrói o nome do arquivo de saída
    # Ex: 'meu_arquivo.txt' -> 'meu_arquivo_U.txt'
    base, extensao = os.path.splitext(caminho_arquivo_entrada)
    caminho_arquivo_saida = f"{base}_U{extensao}"

    linhas_vistas = set()
    linhas_duplicadas = 0
    linhas_unicas = 0

    print(f"Processando o arquivo: {caminho_arquivo_entrada}...")

    try:
        # Abre o arquivo de entrada para leitura e o de saída para escrita
        with open(caminho_arquivo_entrada, 'r', encoding='utf-8') as f_entrada, \
                open(caminho_arquivo_saida, 'w', encoding='utf-8') as f_saida:

            for linha in f_entrada:
                # Verifica se a linha (com seus espaços e quebras de linha)
                # já foi adicionada ao nosso conjunto de linhas vistas.
                if linha not in linhas_vistas:
                    # Se for uma linha nova, escreva no arquivo de saída
                    f_saida.write(linha)
                    # E adicione ao conjunto para futuras verificações
                    linhas_vistas.add(linha)
                    linhas_unicas += 1
                else:
                    # Se a linha já foi vista, apenas contabilizamos
                    linhas_duplicadas += 1

        print("\n--- Processo Concluído ---")
        print(f"Arquivo de saída criado em: {caminho_arquivo_saida}")
        print(f"Linhas únicas encontradas: {linhas_unicas}")
        print(f"Linhas duplicadas removidas: {linhas_duplicadas}")

    except Exception as e:
        print(f"Ocorreu um erro durante o processamento: {e}")


# Coloque aqui o nome do seu arquivo grande
# O arquivo deve estar na mesma pasta que o script Python,
# ou você pode especificar o caminho completo (ex: "C:/Users/SeuUsuario/Documentos/dados.csv")
nome_do_arquivo = r"C:\EngDb\Eneva\Code\CompareFiles\txt_BQ\20250804\hml-g1\Hml_cdhdr_comp_20250804_221756.txt"

remover_duplicatas(nome_do_arquivo)