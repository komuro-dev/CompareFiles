#!/usr/bin/env python3

import os
import sys
from datetime import datetime
from multiprocessing import cpu_count
from typing import Optional, List

# Importa o módulo de validação
try:
    from validacao import CSVComparatorParallel
except ImportError as e:
    print(f"❌ Erro ao importar validacao.py: {e}")
    print("   Certifique-se de que o arquivo validacao.py está no mesmo diretório")
    sys.exit(1)


def find_files_by_basename(directory: str, base_name: str) -> List[str]:
    """
    Encontra arquivos em um diretório que começam com o nome base fornecido.
    A busca ignora maiúsculas/minúsculas.
    Retorna uma lista de caminhos completos para os arquivos encontrados.
    """
    if not os.path.isdir(directory):
        print(f"❌ Diretório não encontrado: {directory}")
        return []

    matching_files = []
    try:
        for filename in os.listdir(directory):
            # Compara o início do nome do arquivo em minúsculas
            if filename.lower().startswith(base_name.lower()):
                matching_files.append(os.path.join(directory, filename))
    except Exception as e:
        print(f"❌ Erro ao acessar o diretório {directory}: {e}")
    return matching_files


def validate_file_exists(file_path: str) -> bool:
    """Valida se o arquivo existe"""
    if not os.path.exists(file_path):
        print(f"❌ Arquivo não encontrado: {file_path}")
        return False
    if not os.path.isfile(file_path):
        print(f"❌ Caminho não é um arquivo: {file_path}")
        return False
    return True


def validate_file_size(file_path: str, max_size_mb: int = 500) -> bool:
    """Valida o tamanho do arquivo"""
    try:
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        print(f"📏 Tamanho do arquivo: {size_mb:.1f} MB")
        if size_mb > max_size_mb:
            print(f"⚠️  Arquivo grande detectado ({size_mb:.1f} MB)")
            print(f"   Processamento pode demorar. Continue? (s/n): ", end="")
            return input().lower().strip() in ['s', 'sim', 'y', 'yes']
        return True
    except Exception as e:
        print(f"❌ Erro ao verificar tamanho do arquivo: {e}")
        return False


def get_performance_recommendations(file1_size: int, file2_size: int) -> dict:
    """Retorna recomendações de performance baseadas no tamanho dos arquivos"""
    total_size_mb = (file1_size + file2_size) / (1024 * 1024)
    if total_size_mb < 50:
        return {'use_parallel': False, 'check_moved_rows': True, 'max_rows_for_moved_check': 20000, 'n_workers': 2,
                'reason': 'Arquivos pequenos'}
    elif total_size_mb < 100:
        return {'use_parallel': True, 'check_moved_rows': True, 'max_rows_for_moved_check': 10000,
                'n_workers': min(4, cpu_count()), 'reason': 'Arquivos médios'}
    else:
        return {'use_parallel': True, 'check_moved_rows': False, 'max_rows_for_moved_check': 5000,
                'n_workers': min(8, cpu_count()), 'reason': 'Arquivos grandes'}


def print_configuration_summary(config: dict):
    """Imprime resumo da configuração"""
    print("\n" + "🔧 CONFIGURAÇÃO ESCOLHIDA".center(70, "="))
    print(f"📁 Arquivo 1: {os.path.basename(config['file1_path'])}")
    print(f"📁 Arquivo 2: {os.path.basename(config['file2_path'])}")
    print(f"🔧 Delimitador: '{config['delimiter']}'")

    excluded_cols = config.get('exclude_columns')
    if excluded_cols:
        print(f"🔪 Colunas Excluídas: {', '.join(map(str, excluded_cols))}")

    print(f"⚡ Paralelismo: {'Ativado' if config['use_parallel'] else 'Desativado'}")
    if config['use_parallel']:
        print(f"👥 Workers: {config['n_workers']}")
    print(f"🔍 Verificar movidas: {'Sim' if config['check_moved_rows'] else 'Não'}")
    if config['check_moved_rows']:
        print(f"📊 Limite movidas: {config['max_rows_for_moved_check']:,} linhas")
    print(f"📊 Normalizar números (legado): {'Sim' if config['normalize_numeric_strings'] else 'Não (Padrão Textual)'}")
    if 'substituir_caractere_invalido' in config:
        print(f"🔄 Padronizar pontuação/caracteres: {'Sim' if config['substituir_caractere_invalido'] else 'Não'}")
    print(f"💡 Motivo: {config['reason']}")
    print("=" * 70)


def _get_first_term_from_filename(file_path: str) -> str:
    """Extrai o primeiro termo de um nome de arquivo"""
    base_name = os.path.basename(file_path)
    return base_name.split('_')[0] if '_' in base_name else os.path.splitext(base_name)[0]


def save_report_to_file(report: dict, output_folder: str, file1_base_name_prefix: str,
                        output_file_override: str = None) -> str:
    """Salva o relatório em arquivo markdown"""
    os.makedirs(output_folder, exist_ok=True)
    if output_file_override is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{file1_base_name_prefix}_{timestamp}.md"
    else:
        output_filename = output_file_override
    output_path = os.path.join(output_folder, output_filename)
    try:
        from markdown_generator import MarkdownGenerator
        md_generator = MarkdownGenerator(report)
        return md_generator.save_report(output_path)
    except ImportError:
        print("⚠️  markdown_generator.py não encontrado. Criando relatório simples...")
        with open(output_path, 'w', encoding='utf-8') as f:
            pass
        return output_path


def save_unmatched_moved_lines_to_file(report: dict, output_folder: str, file1_base_name_prefix: str, delimiter: str) -> \
        Optional[str]:
    """Salva as linhas não movidas/não encontradas em um arquivo de texto separado."""
    os.makedirs(output_folder, exist_ok=True)
    unmatched_lines = report.get('comparacao_linhas', {}).get('linhas_nao_movidas_nao_encontradas', [])
    if unmatched_lines:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{file1_base_name_prefix}_NaoEncontradas_{timestamp}.txt"
        output_path = os.path.join(output_folder, output_filename)
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                for line_info in unmatched_lines:
                    f.write(line_info.get('conteudo_original', '') + '\n')
            return output_path
        except Exception as e:
            print(f"❌ Erro ao salvar '{output_path}': {e}")
    return None


def main():
    """Função principal do sistema"""
    print("🚀 COMPARADOR DE ARQUIVOS CSV COM PADRONIZAÇÃO TEXTUAL")
    print("=" * 70)
    print(f"🖥️  Sistema: {cpu_count()} cores disponíveis")
    print(f"📅 Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 70)

    # ==================== CONFIGURAÇÕES ====================
    CONFIG = {
        'dir1_path': r'C:\EngDb\Eneva\Code\CompareFiles\txt_SAP',  # SAP
        'dir2_path': r'C:\EngDb\Eneva\Code\CompareFiles\txt_BQ\20250814',  # BQ

        'file1_path': None,
        'file2_path': None,

        'delimiter': '!@#',
        'use_parallel': True,
        'check_moved_rows': True,
        'max_rows_for_moved_check': 750000,
        'n_workers': 8,
        'normalize_numeric_strings': False,
        'substituir_caractere_invalido': True,
        'max_file_size_mb': 500,
        'auto_save_report': True,
        'output_folder': 'report',
        'output_file': None,
        'exclude_columns': []
    }
    # =========================================================

    base_name = input("🔍 Digite o nome base do arquivo para procurar (ex: ADRC): ").strip()
    if not base_name:
        print("❌ Nome base não pode ser vazio. Aplicação encerrada.")
        return False

    print("\n" + "─" * 70)
    print(f"🔎 Procurando por arquivos que começam com '{base_name}'...")

    found_files1 = find_files_by_basename(CONFIG['dir1_path'], base_name)
    found_files2 = find_files_by_basename(CONFIG['dir2_path'], base_name)

    error_found = False
    if not found_files1:
        print(f"❌ Nenhum arquivo encontrado em '{CONFIG['dir1_path']}'")
        error_found = True
    elif len(found_files1) > 1:
        print(f"⚠️ Múltiplos arquivos encontrados em '{CONFIG['dir1_path']}':")
        for f in found_files1: print(f"   - {os.path.basename(f)}")
        error_found = True

    if not found_files2:
        print(f"❌ Nenhum arquivo encontrado em '{CONFIG['dir2_path']}'")
        error_found = True
    elif len(found_files2) > 1:
        print(f"⚠️ Múltiplos arquivos encontrados em '{CONFIG['dir2_path']}':")
        for f in found_files2: print(f"   - {os.path.basename(f)}")
        error_found = True

    if error_found:
        print("\n🛑 Processo interrompido. Resolva a ambiguidade de arquivos e tente novamente.")
        return False

    CONFIG['file1_path'] = found_files1[0]
    CONFIG['file2_path'] = found_files2[0]

    print("\n✅ Arquivos selecionados para comparação:")
    print(f"   - Arquivo 1: {os.path.basename(CONFIG['file1_path'])}")
    print(f"   - Arquivo 2: {os.path.basename(CONFIG['file2_path'])}")
    print("─" * 70)

    cols_to_exclude_str = input(
        "🔪 Deseja excluir colunas da comparação? \n   Digite os números das colunas (base 0, ex: 0,5,12) ou deixe em branco para não excluir: ").strip()

    if cols_to_exclude_str:
        try:
            excluded_cols_list = [int(col.strip()) for col in cols_to_exclude_str.split(',')]
            CONFIG['exclude_columns'] = sorted(list(set(excluded_cols_list)))
        except ValueError:
            print(
                "⚠️  Entrada inválida. As colunas não serão excluídas. Apenas números separados por vírgula são permitidos.")

    print("\n📋 Validando arquivos...")
    if not validate_file_exists(CONFIG['file1_path']) or not validate_file_exists(CONFIG['file2_path']):
        return False
    if not validate_file_size(CONFIG['file1_path'], CONFIG['max_file_size_mb']) or not validate_file_size(
            CONFIG['file2_path'], CONFIG['max_file_size_mb']):
        return False

    CONFIG['reason'] = 'Configuração manual'
    print_configuration_summary(CONFIG)

    print("\n🚀 Iniciando comparação...")
    try:
        comparator = CSVComparatorParallel(
            file1_path=CONFIG['file1_path'],
            file2_path=CONFIG['file2_path'],
            delimiter=CONFIG['delimiter'],
            use_parallel=CONFIG['use_parallel'],
            check_moved_rows=CONFIG['check_moved_rows'],
            max_rows_for_moved_check=CONFIG['max_rows_for_moved_check'],
            n_workers=CONFIG['n_workers'],
            normalize_numeric_strings=CONFIG['normalize_numeric_strings'],
            substituir_caractere_invalido=CONFIG['substituir_caractere_invalido'],
            exclude_columns=CONFIG['exclude_columns']
        )

        if not comparator.load_files():
            print("❌ Falha ao carregar os arquivos")
            return False

        report = comparator.generate_report()
        comparator.print_summary()

        file1_prefix = _get_first_term_from_filename(CONFIG['file1_path'])
        if CONFIG['auto_save_report']:
            print("\n💾 Salvando relatório...")
            output_path_md = save_report_to_file(report, CONFIG['output_folder'], file1_prefix, CONFIG['output_file'])
            print(f"📄 Relatório principal salvo em: {output_path_md}")
            output_path_unmatched = save_unmatched_moved_lines_to_file(report, CONFIG['output_folder'], file1_prefix,
                                                                       CONFIG['delimiter'])
            if output_path_unmatched:
                print(f"📄 Linhas não encontradas salvas em: {output_path_unmatched}")

        print("\n✅ Comparação concluída com sucesso!")
        return True
    except KeyboardInterrupt:
        print("\n⚠️  Operação interrompida pelo usuário (Ctrl+C)")
        return False
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    sys.exit(0 if main() else 1)