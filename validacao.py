#!/usr/bin/env python3
"""
Módulo de Validação e Comparação de Arquivos CSV
(Versão com cálculo de linhas distintas)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
import os
import warnings
import sys
import time
import hashlib
from collections import defaultdict
from multiprocessing import Pool, cpu_count

warnings.filterwarnings('ignore')


# ==================== FUNÇÕES AUXILIARES GLOBAIS ====================
def create_row_hash(row_data: List) -> str:
    row_str = '|'.join(str(val) for val in row_data)
    return hashlib.md5(row_str.encode()).hexdigest()


# ==================== CLASSE PRINCIPAL ====================
class CSVComparatorParallel:
    def __init__(self, file1_path: str, file2_path: str, delimiter: str = ',',
                 use_parallel: bool = True, check_moved_rows: bool = True,
                 max_rows_for_moved_check: int = 10000, n_workers: int = None,
                 normalize_numeric_strings: bool = False,
                 substituir_caractere_invalido: bool = False,
                 exclude_columns: Optional[List[int]] = None):
        self.file1_path = file1_path
        self.file2_path = file2_path
        self.delimiter = delimiter
        self.use_parallel = use_parallel
        self.check_moved_rows = check_moved_rows
        self.max_rows_for_moved_check = max_rows_for_moved_check
        self.n_workers = n_workers or min(cpu_count(), 8)
        self.normalize_numeric_strings = normalize_numeric_strings
        self.substituir_caractere_invalido = substituir_caractere_invalido
        self.exclude_columns = exclude_columns if exclude_columns is not None else []
        self.df1 = None
        self.df2 = None
        self.report = {}

    def _replace_invalid_char_in_df(self, df: pd.DataFrame) -> pd.DataFrame:
        print("🔄  Padronizando pontuação e neutralizando caracteres especiais...")
        regex_pattern = r'[^a-zA-Z0-9\s.,;:_()/-]'

        for col in df.select_dtypes(include=['object']).columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(',', '.', regex=False)
                df[col] = df[col].str.replace('\u00A0', '', regex=False)
                df[col] = df[col].str.replace('\u0020', '', regex=False)
                df[col] = df[col].str.replace(regex_pattern, '.', regex=True)
        return df

    def print_progress_bar(self, current: int, total: int, prefix: str = '', suffix: str = '', length: int = 50):
        if total == 0: return
        percent = ("{0:.1f}").format(100 * (current / float(total)))
        filled_length = int(length * current // total)
        bar = '█' * filled_length + '-' * (length - filled_length)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end='', flush=True)
        if current == total: print()

    def create_row_hash_from_series(self, row: pd.Series) -> str:
        return create_row_hash(row.values.tolist())

    def build_hash_index(self, df: pd.DataFrame, desc: str = "") -> Dict[str, List[int]]:
        hash_index = defaultdict(list)
        total_rows = len(df)
        print(f"📋 Construindo índice hash {desc}...")
        for idx in range(total_rows):
            if idx % 2000 == 0 or idx == total_rows - 1:
                self.print_progress_bar(idx + 1, total_rows, prefix=f'🔨 Índice {desc}:',
                                        suffix=f'({idx + 1:,}/{total_rows:,})')
            row_hash = create_row_hash(df.iloc[idx].values.tolist())
            hash_index[row_hash].append(idx)
        print(f"✅ Índice {desc} criado: {len(hash_index):,} hashes únicos")
        return dict(hash_index)

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        print("🧹 Limpando espaços em branco das bordas...")
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()
        return df

    # ===============================================================================
    # FUNÇÃO CORRIGIDA / ADICIONADA
    # Esta função estava faltando e causou o erro.
    # ===============================================================================
    def load_file_with_encoding_fallback(self, file_path: str, file_num: int) -> Tuple[pd.DataFrame, str]:
        encodings_to_try = ['utf-8', 'latin1', 'cp1252']
        for encoding in encodings_to_try:
            try:
                # Carrega tudo como string para evitar inferência de tipo do Pandas
                df = pd.read_csv(file_path, header=None, sep=self.delimiter, encoding=encoding, dtype=str,
                                 engine='python', keep_default_na=False)
                print(f"📂 Arquivo {file_num} carregado com encoding: {encoding}")
                return df, encoding
            except (UnicodeDecodeError, TypeError):
                continue
        raise Exception(f"Não foi possível carregar o arquivo com nenhum encoding testado: {file_path}")

    def load_files(self) -> bool:
        try:
            print("--- Carregando Arquivo 1 ---")
            self.df1_original, self.encoding1 = self.load_file_with_encoding_fallback(self.file1_path, 1)

            if self.exclude_columns:
                print(f"🔪 Excluindo {len(self.exclude_columns)} coluna(s) do Arquivo 1...")
                self.df1_original.drop(columns=self.exclude_columns, inplace=True, errors='ignore')

            self.df1_original = self._clean_dataframe(self.df1_original)
            if self.substituir_caractere_invalido:
                self.df1_original = self._replace_invalid_char_in_df(self.df1_original)

            self.df1 = self.df1_original.copy()

            print("\n--- Carregando Arquivo 2 ---")
            self.df2_original, self.encoding2 = self.load_file_with_encoding_fallback(self.file2_path, 2)

            if self.exclude_columns:
                print(f"🔪 Excluindo {len(self.exclude_columns)} coluna(s) do Arquivo 2...")
                self.df2_original.drop(columns=self.exclude_columns, inplace=True, errors='ignore')

            self.df2_original = self._clean_dataframe(self.df2_original)
            if self.substituir_caractere_invalido:
                self.df2_original = self._replace_invalid_char_in_df(self.df2_original)

            self.df2 = self.df2_original.copy()

            self.df1.columns = range(self.df1.shape[1])
            self.df2.columns = range(self.df2.shape[1])
            self.df1_original.columns = range(self.df1_original.shape[1])
            self.df2_original.columns = range(self.df2_original.shape[1])

            print(f"\n✓ Arquivo 1: {self.df1.shape[0]:,} linhas × {self.df1.shape[1]} colunas (após exclusão)")
            print(f"✓ Arquivo 2: {self.df2.shape[0]:,} linhas × {self.df2.shape[1]} colunas (após exclusão)")
            return True
        except Exception as e:
            print(f"❌ Erro ao carregar arquivos: {e}")
            import traceback
            traceback.print_exc()
            return False

    def compare_columns(self) -> Dict:
        cols1, cols2 = self.df1.columns, self.df2.columns
        set1, set2 = set(cols1), set(cols2)
        return {'arquivo1': {'quantidade': len(cols1), 'posicoes': list(cols1)},
                'arquivo2': {'quantidade': len(cols2), 'posicoes': list(cols2)},
                'mesma_quantidade': len(cols1) == len(cols2), 'diferenca_quantidade': abs(len(cols1) - len(cols2)),
                'posicoes_apenas_arquivo1': list(set1 - set2), 'posicoes_apenas_arquivo2': list(set2 - set1),
                'posicoes_comuns': list(set1 & set2), 'colunas_diferentes': bool(set1 ^ set2)}

    def compare_data_types(self) -> Dict:
        if self.df1.shape[1] != self.df2.shape[1]: return {"erro": "Quantidade de colunas diferente"}
        types1, types2 = self.df1.dtypes, self.df2.dtypes
        diferencas = {f'posicao_{i}': {'arquivo1': str(t1), 'arquivo2': str(t2)} for i, (t1, t2) in
                      enumerate(zip(types1, types2)) if t1 != t2}
        return {'arquivo1': {f'posicao_{i}': str(t) for i, t in enumerate(types1)},
                'arquivo2': {f'posicao_{i}': str(t) for i, t in enumerate(types2)}, 'diferencas': diferencas,
                'tipos_identicos': not bool(diferencas)}

    def compare_rows_count(self) -> Dict:
        return {'arquivo1': len(self.df1), 'arquivo2': len(self.df2),
                'mesma_quantidade': len(self.df1) == len(self.df2), 'diferenca': abs(len(self.df1) - len(self.df2))}

    def compare_rows_detailed(self) -> Dict:
        if self.df1.shape[1] != self.df2.shape[1]: return {"erro": "Quantidade de colunas diferente"}
        result = {'linhas_identicas_posicao': [], 'linhas_diferentes_posicao': [], 'linhas_apenas_arquivo1': [],
                  'linhas_apenas_arquivo2': [], 'linhas_movidas': [], 'linhas_nao_movidas_nao_encontradas': []}

        print("\n📊 ETAPA 1: Calculando contagens de linhas distintas...")
        hash_index_2 = self.build_hash_index(self.df2, "Arquivo 2 (Contagem)")
        self.report['linhas']['distintas_arquivo2'] = len(hash_index_2)

        min_rows = min(len(self.df1), len(self.df2))

        print(f"\n📊 ETAPA 2: Coletando todas as {min_rows:,} linhas da intersecção para busca por conteúdo.")
        self._compare_positional_sequential(result, min_rows)

        print(f"\n📊 ETAPA 3: Identificando linhas exclusivas")
        self._identify_exclusive_rows(result, min_rows)

        if self.check_moved_rows:
            self._find_moved_rows(result, hash_index_2)
        else:
            self._mark_different_as_exclusive(result)

        self._calculate_final_statistics(result)
        return result

    def _compare_positional_sequential(self, result: Dict, min_rows: int):
        """Prepara todas as linhas da intersecção para a busca por conteúdo via hash."""
        result['linhas_diferentes_posicao'] = [{'linha': i} for i in range(min_rows)]
        self.print_progress_bar(min_rows, min_rows, prefix='📊 Coletando linhas para busca:')

    def _identify_exclusive_rows(self, result: Dict, min_rows: int):
        for linha in range(min_rows, len(self.df1_original)):
            result['linhas_apenas_arquivo1'].append({
                'linha': linha,
                'dados_por_posicao': {pos: val for pos, val in enumerate(self.df1_original.iloc[linha])}
            })
        for linha in range(min_rows, len(self.df2_original)):
            result['linhas_apenas_arquivo2'].append({
                'linha': linha,
                'dados_por_posicao': {pos: val for pos, val in enumerate(self.df2_original.iloc[linha])}
            })

    def _find_moved_rows(self, result: Dict, hash_index_2: Dict):
        rows_to_check_indices = [item['linha'] for item in result['linhas_diferentes_posicao']]
        if not rows_to_check_indices:
            return

        if len(rows_to_check_indices) > self.max_rows_for_moved_check:
            print(
                f"\n⚠️  Busca por conteúdo LIMITADA: A verificação será restrita a {self.max_rows_for_moved_check:,} (de {len(rows_to_check_indices):,}) linhas.")
            ignored_diffs = result['linhas_diferentes_posicao'][self.max_rows_for_moved_check:]
            for diff_item in ignored_diffs:
                result['linhas_nao_movidas_nao_encontradas'].append({
                    'linha': diff_item['linha'],
                    'conteudo_original': self.delimiter.join(map(str, self.df1_original.iloc[diff_item['linha']]))
                })
            rows_to_check_indices = rows_to_check_indices[:self.max_rows_for_moved_check]

        print(f"\n📊 ETAPA 4: Buscando por {len(rows_to_check_indices):,} linhas do Arquivo 1 no Arquivo 2 (via hash)")
        self._find_moved_rows_logic(result, rows_to_check_indices, hash_index_2)

    def _find_moved_rows_logic(self, result: Dict, rows_to_check_indices: List[int], hash_index_2: Dict):
        for idx, i in enumerate(rows_to_check_indices):
            if idx % 200 == 0 or idx == len(rows_to_check_indices) - 1:
                self.print_progress_bar(idx + 1, len(rows_to_check_indices), prefix='🔍 Buscando por conteúdo:')

            row_hash = self.create_row_hash_from_series(self.df1.iloc[i])
            matching_positions = hash_index_2.get(row_hash, [])
            original_row_content = self.delimiter.join(map(str, self.df1_original.iloc[i]))

            if matching_positions:
                result['linhas_movidas'].append({
                    'linha_original': i,
                    'posicoes_encontradas': matching_positions,
                    'conteudo_original': original_row_content
                })
            else:
                result['linhas_nao_movidas_nao_encontradas'].append({
                    'linha': i,
                    'conteudo_original': original_row_content
                })

        result['linhas_diferentes_posicao'] = []

    def _mark_different_as_exclusive(self, result: Dict):
        for diff_entry in result['linhas_diferentes_posicao']:
            linha_idx = diff_entry['linha']
            result['linhas_apenas_arquivo1'].append({
                'linha': linha_idx,
                'dados_por_posicao': {pos: val for pos, val in enumerate(self.df1_original.iloc[linha_idx])}
            })
            result['linhas_apenas_arquivo2'].append({
                'linha': linha_idx,
                'dados_por_posicao': {pos: val for pos, val in enumerate(self.df2_original.iloc[linha_idx])}
            })
        result['linhas_diferentes_posicao'] = []

    def _calculate_final_statistics(self, result: Dict):
        total_linhas = max(len(self.df1), len(self.df2))
        if total_linhas > 0:
            result['percentual_similaridade'] = (len(result['linhas_movidas']) / total_linhas) * 100
        else:
            result['percentual_similaridade'] = 100.0

    def generate_report(self) -> Dict:
        print("\n" + "=" * 25 + " INICIANDO COMPARAÇÃO " + "=" * 25)
        start_total = time.time()
        self.report['metadados'] = {'arquivo1': {'caminho': self.file1_path, 'nome': os.path.basename(self.file1_path),
                                                 'tamanho': os.path.getsize(self.file1_path)},
                                    'arquivo2': {'caminho': self.file2_path, 'nome': os.path.basename(self.file2_path),
                                                 'tamanho': os.path.getsize(self.file2_path)},
                                    'configuracao': {'delimitador': self.delimiter, 'paralelismo': self.use_parallel,
                                                     'workers': self.n_workers if self.use_parallel else 1,
                                                     'verificacao_movidas': self.check_moved_rows,
                                                     'max_linhas_movidas': self.max_rows_for_moved_check,
                                                     'normalize_numeric_strings': self.normalize_numeric_strings,
                                                     'substituir_caractere_invalido': self.substituir_caractere_invalido,
                                                     'exclude_columns': self.exclude_columns},
                                    'data_comparacao': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        print("\n🔍 Comparando estrutura...")
        self.report['colunas'] = self.compare_columns()
        self.report['tipos_dados'] = self.compare_data_types()
        self.report['linhas'] = self.compare_rows_count()
        if not self.report['colunas']['colunas_diferentes']:
            self.report['comparacao_linhas'] = self.compare_rows_detailed()
        else:
            print("\n⚠️  Pulando comparação detalhada de conteúdo - colunas diferentes.")
            self.report['comparacao_linhas'] = {"erro": "Quantidade de colunas diferente",
                                                'linhas_identicas_posicao': [], 'linhas_diferentes_posicao': [],
                                                'linhas_apenas_arquivo1': [], 'linhas_apenas_arquivo2': [],
                                                'linhas_movidas': [], 'linhas_nao_movidas_nao_encontradas': []}

        if 'distintas_arquivo1' not in self.report['linhas']:
            self.report['linhas']['distintas_arquivo1'] = len(self.build_hash_index(self.df1, "Arquivo 1 (Contagem)"))
        if 'distintas_arquivo2' not in self.report['linhas']:
            self.report['linhas']['distintas_arquivo2'] = len(self.build_hash_index(self.df2, "Arquivo 2 (Contagem)"))

        end_total = time.time()
        self.report['metadados']['tempo_execucao'] = f"{end_total - start_total:.2f}s"
        print(f"\n✅ Comparação concluída! Tempo total: {self.report['metadados']['tempo_execucao']}")
        return self.report

    def print_summary(self):
        print("\n" + "📊 RESUMO DA COMPARAÇÃO".center(80, "="))
        print(f"📁 Arquivo 1: {os.path.basename(self.file1_path)}")
        print(f"📁 Arquivo 2: {os.path.basename(self.file2_path)}")
        if self.exclude_columns:
            print(f"🔪 Colunas Excluídas: {', '.join(map(str, self.exclude_columns))}")
        print(f"📅 Data: {self.report['metadados']['data_comparacao']}")
        print(f"⏱️  Tempo: {self.report['metadados']['tempo_execucao']}")
        print("-" * 80)
        col_info = self.report.get('colunas', {})
        rows_info = self.report.get('linhas', {})
        types_info = self.report.get('tipos_dados', {})
        print("🔍 ESTRUTURA:")
        print(
            f"  - Colunas: {col_info.get('arquivo1', {}).get('quantidade', 0)} vs {col_info.get('arquivo2', {}).get('quantidade', 0)} -> {'✅ Iguais' if col_info.get('mesma_quantidade') else '❌ Diferentes'}")
        print(
            f"  - Tipos de Dados: {'✅ Idênticos (Todos como Texto)' if types_info.get('tipos_identicos') else '❌ Diferentes'}")
        print(
            f"  - Linhas: {rows_info.get('arquivo1', 0):,} vs {rows_info.get('arquivo2', 0):,} -> {'✅ Iguais' if rows_info.get('mesma_quantidade') else '❌ Diferentes'}")
        print(f"    - Distintas Arq 1: {rows_info.get('distintas_arquivo1', 0):,}")
        print(f"    - Distintas Arq 2: {rows_info.get('distintas_arquivo2', 0):,}")
        print()
        if 'comparacao_linhas' in self.report and 'erro' not in self.report['comparacao_linhas']:
            line_comp = self.report['comparacao_linhas']
            movidas_arq1 = len(line_comp.get('linhas_movidas', []))
            nao_encontradas_arq1 = len(line_comp.get('linhas_nao_movidas_nao_encontradas', [])) + len(
                line_comp.get('linhas_apenas_arquivo1', []))
            nao_encontradas_arq2 = len(line_comp.get('linhas_apenas_arquivo2', []))

            print("📊 CONTEÚDO:")
            print(f"  - Linhas Encontradas (Arq 1 -> Arq 2): {movidas_arq1:,}")
            print(f"  - Linhas Não Encontradas (Arq 1 -> Arq 2): {nao_encontradas_arq1:,}")
            print(f"  - Linhas Não Encontradas (Arq 2 -> Arq 1): {nao_encontradas_arq2:,}")

        else:
            print("⚠️  Comparação detalhada de conteúdo não realizada.")
        print("=" * 80)