import os
from typing import Dict, Any


class MarkdownGenerator:
    def __init__(self, report_data: Dict):
        self.report = report_data

    def _safe_get(self, dictionary: Dict, key: str, default=None):
        """Método auxiliar para acessar dados com segurança"""
        return dictionary.get(key, default)

    def generate_markdown_report(self) -> str:
        """Gera relatório em formato Markdown"""
        md_content = []

        if not self.report:
            return "# Erro: Dados de relatório não fornecidos"

        md_content.append("# 📊 Relatório de Comparação de Arquivos CSV")
        md_content.append("*Análise de arquivos sem cabeçalhos - referências por posição de coluna*")
        md_content.append("")

        metadados = self._safe_get(self.report, 'metadados', {})
        configuracao = self._safe_get(metadados, 'configuracao', {})
        data_comparacao = self._safe_get(metadados, 'data_comparacao', 'Data não disponível')
        md_content.append(f"**Data da Comparação:** {data_comparacao}")

        excluded_columns = self._safe_get(configuracao, 'exclude_columns', [])
        if excluded_columns:
            md_content.append(f"**Colunas Excluídas da Análise (base 0):** `{', '.join(map(str, excluded_columns))}`")

        md_content.append("")

        self._add_file_info(md_content, metadados)
        self._add_executive_summary(md_content)
        self._add_column_analysis(md_content)
        self._add_data_types_analysis(md_content)
        # Ordem alterada para chamar a nova função no local correto
        self._add_rows_analysis(md_content)
        self._add_detailed_line_comparison(md_content)
        self._add_footer(md_content)

        return "\n".join(md_content)

    def _add_file_info(self, md_content: list, metadados: Dict):
        """Adiciona informações dos arquivos"""
        md_content.append("## 📁 Arquivos Analisados")
        md_content.append("")
        md_content.append("| Arquivo | Caminho | Tamanho |")
        md_content.append("|---------|---------|---------|")

        for i, arquivo_key in enumerate(['arquivo1', 'arquivo2'], 1):
            arquivo_info = self._safe_get(metadados, arquivo_key, {})
            caminho = self._safe_get(arquivo_info, 'caminho', 'N/A')
            tamanho = self._safe_get(arquivo_info, 'tamanho', 0)
            tamanho_kb = tamanho / 1024 if tamanho > 0 else 0
            md_content.append(f"| Arquivo {i} | `{caminho}` | {tamanho_kb:.1f} KB |")

        md_content.append("")

    def _add_executive_summary(self, md_content: list):
        """Adiciona resumo executivo"""
        md_content.append("## 📈 Resumo Executivo")
        md_content.append("")

        col_info = self._safe_get(self.report, 'colunas', {})
        rows_info = self._safe_get(self.report, 'linhas', {})
        types_info = self._safe_get(self.report, 'tipos_dados', {})

        mesma_qtd_colunas = self._safe_get(col_info, 'mesma_quantidade', False)
        status_colunas = "✅ Mesma quantidade" if mesma_qtd_colunas else "❌ Quantidades diferentes"

        tipos_identicos = self._safe_get(types_info, 'tipos_identicos', False)
        tem_erro_tipos = 'erro' in types_info
        status_tipos = "✅ Idênticos (Texto)" if not tem_erro_tipos and tipos_identicos else "❌ Diferentes"

        mesma_qtd_linhas = self._safe_get(rows_info, 'mesma_quantidade', False)
        status_linhas = "✅ Mesma quantidade" if mesma_qtd_linhas else "❌ Quantidades diferentes"

        qtd_col1 = self._safe_get(col_info.get('arquivo1', {}), 'quantidade', 0)
        qtd_col2 = self._safe_get(col_info.get('arquivo2', {}), 'quantidade', 0)
        qtd_lin1 = self._safe_get(rows_info, 'arquivo1', 0)
        qtd_lin2 = self._safe_get(rows_info, 'arquivo2', 0)
        qtd_dif_tipos = len(self._safe_get(types_info, 'diferencas', {}))

        md_content.append("| Aspecto | Status | Detalhes |")
        md_content.append("|---------|--------|----------|")
        md_content.append(f"| Quantidade de Colunas (após exclusão) | {status_colunas} | {qtd_col1} vs {qtd_col2} |")
        md_content.append(f"| Tipos de Dados | {status_tipos} | {qtd_dif_tipos} diferenças |")
        md_content.append(f"| Quantidade de Linhas | {status_linhas} | {qtd_lin1:,} vs {qtd_lin2:,} |")

        md_content.append("")

    def _add_column_analysis(self, md_content: list):
        """Adiciona análise das colunas"""
        md_content.append("## 🔍 Análise das Colunas (Após Exclusão)")
        md_content.append("")
        # ... (código existente sem alterações)
        col_info = self._safe_get(self.report, 'colunas', {})
        md_content.append("### Informações Gerais")
        md_content.append("")
        md_content.append("| Arquivo | Quantidade | Posições |")
        md_content.append("|---------|------------|----------|")
        arquivo1_info = self._safe_get(col_info, 'arquivo1', {})
        qtd1 = self._safe_get(arquivo1_info, 'quantidade', 0)
        pos1 = self._safe_get(arquivo1_info, 'posicoes', [])
        pos1_str = ", ".join([f"Col{pos}" for pos in pos1]) if pos1 else "Nenhuma"
        arquivo2_info = self._safe_get(col_info, 'arquivo2', {})
        qtd2 = self._safe_get(arquivo2_info, 'quantidade', 0)
        pos2 = self._safe_get(arquivo2_info, 'posicoes', [])
        pos2_str = ", ".join([f"Col{pos}" for pos in pos2]) if pos2 else "Nenhuma"
        md_content.append(f"| Arquivo 1 | {qtd1} | {pos1_str} |")
        md_content.append(f"| Arquivo 2 | {qtd2} | {pos2_str} |")
        md_content.append("")
        colunas_diferentes = self._safe_get(col_info, 'colunas_diferentes', False)
        if colunas_diferentes:
            md_content.append("### ⚠️ Diferenças nas Colunas")
            md_content.append("")
            diferenca_qtd = self._safe_get(col_info, 'diferenca_quantidade', 0)
            md_content.append(f"**Diferença na quantidade:** {diferenca_qtd} coluna(s)")
            md_content.append("")
            pos_apenas_1 = self._safe_get(col_info, 'posicoes_apenas_arquivo1', [])
            if pos_apenas_1:
                md_content.append("**Posições apenas no Arquivo 1:** " + ", ".join(map(str, pos_apenas_1)))
            pos_apenas_2 = self._safe_get(col_info, 'posicoes_apenas_arquivo2', [])
            if pos_apenas_2:
                md_content.append("**Posições apenas no Arquivo 2:** " + ", ".join(map(str, pos_apenas_2)))
            md_content.append("")

    def _add_data_types_analysis(self, md_content: list):
        """Adiciona análise dos tipos de dados"""
        md_content.append("## 🔧 Análise dos Tipos de Dados")
        md_content.append("")
        # ... (código existente sem alterações)
        types_info = self._safe_get(self.report, 'tipos_dados', {})
        if 'erro' in types_info:
            md_content.append(f"❌ **Erro:** {types_info['erro']}")
            md_content.append("")
            return
        tipos_identicos = self._safe_get(types_info, 'tipos_identicos', True)
        if tipos_identicos:
            md_content.append("✅ **Todos os tipos de dados são idênticos (tratados como Texto).**")
        else:
            md_content.append("❌ **Diferenças nos tipos de dados encontradas:**")
            md_content.append("")
            diferencas = self._safe_get(types_info, 'diferencas', {})
            if diferencas:
                md_content.append("| Posição | Arquivo 1 | Arquivo 2 |")
                md_content.append("|---------|-----------|-----------|")
                for pos_key, diff in diferencas.items():
                    pos_num = pos_key.replace('posicao_', '')
                    tipo1 = self._safe_get(diff, 'arquivo1', 'N/A')
                    tipo2 = self._safe_get(diff, 'arquivo2', 'N/A')
                    md_content.append(f"| Coluna {pos_num} | {tipo1} | {tipo2} |")
        md_content.append("")

    # RELATÓRIO ALTERADO: Esta função agora inclui a nova seção de análise de duplicação.
    def _add_rows_analysis(self, md_content: list):
        """Adiciona análise das linhas, incluindo totais e contagens distintas."""
        md_content.append("## 📊 Análise das Linhas")
        md_content.append("")

        rows_info = self._safe_get(self.report, 'linhas', {})
        md_content.append("### Informações Gerais")
        md_content.append("")
        md_content.append("| Métrica | Arquivo 1 | Arquivo 2 | Diferença |")
        md_content.append("|---------|-----------|-----------|-----------|")
        qtd1 = self._safe_get(rows_info, 'arquivo1', 0)
        qtd2 = self._safe_get(rows_info, 'arquivo2', 0)
        diferenca = self._safe_get(rows_info, 'diferenca', 0)
        md_content.append(f"| Quantidade de Linhas | {qtd1:,} | {qtd2:,} | {diferenca:,} |")
        md_content.append("")

        # NOVA SEÇÃO ADICIONADA AQUI
        md_content.append("### Análise de Duplicação")
        md_content.append("")
        md_content.append("| Arquivo | Total de Linhas | Linhas com Conteúdo Distinto | Percentual de Duplicação |")
        md_content.append("|---------|-----------------|------------------------------|--------------------------|")

        distintas1 = self._safe_get(rows_info, 'distintas_arquivo1', 0)
        distintas2 = self._safe_get(rows_info, 'distintas_arquivo2', 0)

        perc_dup1 = (1 - (distintas1 / qtd1)) * 100 if qtd1 > 0 else 0
        perc_dup2 = (1 - (distintas2 / qtd2)) * 100 if qtd2 > 0 else 0

        md_content.append(f"| Arquivo 1 | {qtd1:,} | {distintas1:,} | {perc_dup1:.1f}% |")
        md_content.append(f"| Arquivo 2 | {qtd2:,} | {distintas2:,} | {perc_dup2:.1f}% |")
        md_content.append("")

    def _add_detailed_line_comparison(self, md_content: list):
        """Adiciona comparação detalhada das linhas com a nova lógica."""
        line_comp = self._safe_get(self.report, 'comparacao_linhas', {})
        if not line_comp or 'erro' in line_comp:
            md_content.append("⚠️ **Comparação detalhada não realizada.**")
            md_content.append("")
            return

        md_content.append("### 🔍 Comparação Detalhada do Conteúdo")
        md_content.append("")

        apenas_1 = len(self._safe_get(line_comp, 'linhas_apenas_arquivo1', []))
        apenas_2 = len(self._safe_get(line_comp, 'linhas_apenas_arquivo2', []))
        movidas = self._safe_get(line_comp, 'linhas_movidas', [])
        nao_movidas_nao_encontradas = len(self._safe_get(line_comp, 'linhas_nao_movidas_nao_encontradas', []))

        contagem_movidas_arq1 = len(movidas)
        indices_encontrados_arq2 = set()
        for movida in movidas:
            indices_encontrados_arq2.update(movida.get('posicoes_encontradas', []))
        contagem_movidas_arq2 = len(indices_encontrados_arq2)

        total_linhas_arq1 = self._safe_get(self.report.get('linhas', {}), 'arquivo1', 0)
        total_linhas_arq2 = self._safe_get(self.report.get('linhas', {}), 'arquivo2', 0)

        md_content.append("| Tipo de Ocorrência | Quantidade | Percentual (sobre o total do arquivo) |")
        md_content.append("|-------------------|------------|---------------------------------------|")

        if total_linhas_arq1 > 0:
            md_content.append(
                f"| 🔄 Linhas do Arquivo 1 encontradas no Arquivo 2 | {contagem_movidas_arq1:,} | {contagem_movidas_arq1 / total_linhas_arq1 * 100:.1f}% |")
        if total_linhas_arq2 > 0:
            md_content.append(
                f"| 🔄 Linhas do Arquivo 2 encontradas no Arquivo 1 | {contagem_movidas_arq2:,} | {contagem_movidas_arq2 / total_linhas_arq2 * 100:.1f}% |")

        total_nao_encontradas_arq1 = nao_movidas_nao_encontradas + apenas_1
        if total_linhas_arq1 > 0:
            md_content.append(
                f"| ❓ Linhas do Arquivo 1 NÃO encontradas no Arquivo 2 | {total_nao_encontradas_arq1:,} | {total_nao_encontradas_arq1 / total_linhas_arq1 * 100:.1f}% |")
        if total_linhas_arq2 > 0:
            total_nao_encontradas_arq2 = total_linhas_arq2 - contagem_movidas_arq2
            md_content.append(
                f"| ❓ Linhas do Arquivo 2 NÃO encontradas no Arquivo 1 | {total_nao_encontradas_arq2:,} | {total_nao_encontradas_arq2 / total_linhas_arq2 * 100:.1f}% |")

        md_content.append("")
        self._add_unmatched_lines_details(md_content, line_comp)

    def _add_unmatched_lines_details(self, md_content: list, line_comp: Dict):
        """Adiciona amostras de linhas não encontradas e exclusivas."""
        md_content.append("### Amostra de Linhas Não Correspondidas")
        md_content.append("")

        md_content.append("#### ❓ Linhas do Arquivo 1 Não Encontradas no Arquivo 2")
        nao_encontradas = self._safe_get(line_comp, 'linhas_nao_movidas_nao_encontradas', [])
        apenas_1 = self._safe_get(line_comp, 'linhas_apenas_arquivo1', [])
        total_nao_encontradas_arq1 = nao_encontradas + apenas_1
        if total_nao_encontradas_arq1:
            for item in total_nao_encontradas_arq1[:5]:
                linha_num = self._safe_get(item, 'linha', 'N/A')
                conteudo = item.get('conteudo_original')
                if not conteudo:
                    delimiter = self.report.get('metadados', {}).get('configuracao', {}).get('delimitador', '|')
                    dados = self._safe_get(item, 'dados_por_posicao', {})
                    conteudo = delimiter.join(map(str, dados.values()))
                md_content.append(f"**Linha Original {linha_num}:**")
                md_content.append(f"```\n{conteudo}\n```")
            if len(total_nao_encontradas_arq1) > 5:
                md_content.append(f"*... e mais {len(total_nao_encontradas_arq1) - 5:,} linhas não encontradas.*")
        else:
            md_content.append("*Nenhuma linha deste tipo foi encontrada.*")
        md_content.append("")

        md_content.append("#### ❓ Linhas do Arquivo 2 Não Encontradas no Arquivo 1")
        apenas_2 = self._safe_get(line_comp, 'linhas_apenas_arquivo2', [])
        if apenas_2:
            for item in apenas_2[:5]:
                linha_num = self._safe_get(item, 'linha', 'N/A')
                delimiter = self.report.get('metadados', {}).get('configuracao', {}).get('delimitador', '|')
                dados = self._safe_get(item, 'dados_por_posicao', {})
                conteudo = delimiter.join(map(str, dados.values()))
                md_content.append(f"**Linha Original {linha_num}:**")
                md_content.append(f"```\n{conteudo}\n```")
            if len(apenas_2) > 5:
                md_content.append(f"*... e mais {len(apenas_2) - 5:,} linhas exclusivas.*")
        else:
            md_content.append("*Nenhuma linha deste tipo foi encontrada.*")
        md_content.append("")

    def _add_footer(self, md_content: list):
        """Adiciona rodapé do relatório"""
        md_content.append("---")
        md_content.append("*Relatório gerado automaticamente pelo Comparador de Arquivos CSV*")

    def save_report(self, output_path: str = "relatorio_comparacao.md") -> str:
        """Salva o relatório em formato Markdown"""
        try:
            markdown_content = self.generate_markdown_report()
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            print(f"📄 Relatório markdown salvo em: {output_path}")
            return output_path
        except Exception as e:
            print(f"❌ Erro ao salvar relatório: {e}")
            return ""


if __name__ == "__main__":
    print("📄 Gerador de Relatório Markdown")
    print("Este arquivo deve ser importado pelo comparador principal.")