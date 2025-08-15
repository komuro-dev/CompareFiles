#!/usr/bin/env python3
"""
Ferramenta para Corrigir Erros de Codificação de Caracteres (Mojibake) em Arquivos de Texto/CSV.
Usa a biblioteca 'ftfy' para "consertar" o texto e salva uma nova cópia do arquivo em UTF-8.
"""

import os
import sys
import codecs  # Módulo para trabalhar com encodings de forma mais robusta

try:
    import ftfy
except ImportError:
    print("=" * 70)
    print("❌ ERRO: A biblioteca 'ftfy' é necessária para este script.")
    print("   Por favor, instale-a executando o comando no seu terminal:")
    print("   pip install ftfy")
    print("=" * 70)
    sys.exit(1)

# ==============================================================================
# ### CONFIGURAÇÕES ###
# Adicione o caminho completo para os arquivos que você deseja corrigir.
# Você pode adicionar múltiplos arquivos na lista, separados por vírgula.
# ==============================================================================

ARQUIVOS_PARA_CORRIGIR = [
    # Exemplo para Windows:
    r'C:\EngDb\Eneva\Code\CompareFiles\txt_SAP\ADRC_20250725_115350.txt',

    # Exemplo para Linux/macOS:
    # '/home/usuario/documentos/arquivo_com_erro.txt',

    # Adicione mais arquivos aqui, se necessário:
    # r'C:\caminho\para\outro_arquivo.txt',
]


# ==============================================================================
# ### FIM DAS CONFIGURAÇÕES ###
# ==============================================================================


def corrigir_arquivo(caminho_arquivo: str):
    """
    Lê um arquivo, corrige os erros de encoding linha por linha e salva em um novo arquivo.
    """
    print("-" * 70)

    # 1. Validar se o arquivo de origem existe
    if not os.path.isfile(caminho_arquivo):
        print(f"⚠️  AVISO: Arquivo não encontrado ou não é um arquivo válido. Pulando:")
        print(f"   '{caminho_arquivo}'")
        return

    print(f"📄 Processando arquivo: '{os.path.basename(caminho_arquivo)}'")

    # 2. Definir o nome do novo arquivo com sufixo "_e"
    diretorio, nome_completo = os.path.split(caminho_arquivo)
    nome, extensao = os.path.splitext(nome_completo)
    novo_nome = f"{nome}_e{extensao}"
    novo_caminho = os.path.join(diretorio, novo_nome)

    print(f"   ↳ Salvando arquivo corrigido como: '{novo_nome}'")

    # 3. Processar o arquivo
    total_linhas = 0
    try:
        # Abre o arquivo de origem assumindo um encoding "problemático" comum (latin-1)
        # e o de destino com o encoding correto (UTF-8).
        with codecs.open(caminho_arquivo, 'r', encoding='latin-1') as f_origem, \
                codecs.open(novo_caminho, 'w', encoding='utf-8') as f_destino:

            for linha in f_origem:
                # A mágica acontece aqui: ftfy conserta a linha
                linha_corrigida = ftfy.fix_text(linha)

                # Escreve a linha corrigida no novo arquivo
                f_destino.write(linha_corrigida)
                total_linhas += 1

                if total_linhas % 20000 == 0:
                    print(f"   ... {total_linhas:,} linhas processadas")

        print(f"✅ Concluído! Arquivo corrigido com {total_linhas:,} linhas salvo com sucesso.")

    except Exception as e:
        print(f"❌ ERRO ao processar o arquivo '{caminho_arquivo}': {e}")
        # Se deu erro, remove o arquivo de destino parcialmente criado
        if os.path.exists(novo_caminho):
            os.remove(novo_caminho)


if __name__ == "__main__":
    print("=" * 70)
    print("⚙️  INICIANDO SCRIPT DE CORREÇÃO DE ENCODING DE ARQUIVOS")
    print("=" * 70)

    if not ARQUIVOS_PARA_CORRIGIR or not ARQUIVOS_PARA_CORRIGIR[0].startswith(('C:', '/', '~')):
        print("⚠️  AVISO: A lista 'ARQUIVOS_PARA_CORRIGIR' está vazia ou contém apenas o exemplo.")
        print("   Por favor, edite o script e adicione o caminho dos arquivos que deseja corrigir.")
        sys.exit(0)

    for arquivo in ARQUIVOS_PARA_CORRIGIR:
        corrigir_arquivo(arquivo)

    print("-" * 70)
    print("✨ Processo finalizado.")
    print("-" * 70)