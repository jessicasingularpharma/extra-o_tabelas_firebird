import firebirdsql
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import io
from tqdm import tqdm
import logging

class DatabaseMigration:
    def __init__(self):
        load_dotenv()

        # Configuração do Firebird
        self.firebird_config = {
            'host': os.getenv("FIREBIRD_HOST"),
            'database': os.getenv("FIREBIRD_DATABASE"),
            'user': os.getenv("FIREBIRD_USER"),
            'password': os.getenv("FIREBIRD_PASSWORD"),
            'charset': 'ISO8859_1'
        }

        # Configuração do PostgreSQL
        self.engine = create_engine(
            f'postgresql+psycopg2://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}/{os.getenv("POSTGRES_DB")}',
            pool_size=5, max_overflow=10
        )

        # Certificar que o schema bronze existe no PostgreSQL
        self.ensure_bronze_schema()

        # Configuração de logging
        logging.basicConfig(filename='migration.log', level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

    def ensure_bronze_schema(self):
        with self.engine.connect() as connection:
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
            connection.commit()

    def create_table_in_postgres(self, table_name, columns):
        column_definitions = ", ".join([f'"{col}" TEXT' for col in columns])
        create_table_query = f'CREATE TABLE IF NOT EXISTS bronze."{table_name}" ({column_definitions});'
        
        with self.engine.connect() as connection:
            connection.execute(text(create_table_query))
            connection.commit()

    def list_firebird_tables(self):
        with firebirdsql.connect(**self.firebird_config) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 0;")
            tables = [row[0].strip() for row in cursor.fetchall()]
        return tables

    def sanitize_dataframe(self, df):
        # Remove caracteres nulos (0x00) em células de texto, mantendo valores NULL inalterados
        return df.apply(lambda col: col.map(lambda x: x.replace('\x00', '') if isinstance(x, str) else x))

    def load_data_using_copy(self, df, table_name):
        df = self.sanitize_dataframe(df)  # Remover caracteres nulos
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        conn = self.engine.raw_connection()
        cursor = conn.cursor()

        try:
            cursor.copy_expert(f'COPY bronze."{table_name}" FROM STDIN WITH CSV', csv_buffer)
            conn.commit()
        except Exception as e:
            print(f"Erro ao carregar bloco para bronze.{table_name}: {e}")
            conn.rollback()
            # Se o carregamento em bloco falhar, tenta inserir linha a linha
            self.load_rows_individually(df, table_name)
        finally:
            cursor.close()
            conn.close()

    def load_rows_individually(self, df, table_name):
        conn = self.engine.raw_connection()
        cursor = conn.cursor()

        for index, row in df.iterrows():
            try:
                csv_buffer = io.StringIO()
                row.to_frame().T.to_csv(csv_buffer, index=False, header=False)
                csv_buffer.seek(0)
                cursor.copy_expert(f'COPY bronze."{table_name}" FROM STDIN WITH CSV', csv_buffer)
                conn.commit()
            except Exception as e:
                print(f"Erro ao carregar linha {index + 1} da tabela {table_name}: {e}")
                conn.rollback()
        cursor.close()
        conn.close()

    def get_destination_row_count(self, table_name):
        """
        Retorna a quantidade de linhas já carregadas na tabela de destino.
        Essa contagem é utilizada para definir o offset e evitar recarregar linhas já migradas.
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(f'SELECT COUNT(*) FROM bronze."{table_name}"'))
            return result.scalar()

    def extract_and_load_data_in_chunks(self, table_name, block_size=10000):
        # Obtém informações da tabela de origem
        with firebirdsql.connect(**self.firebird_config) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_records = cursor.fetchone()[0]

            cursor.execute(f"SELECT * FROM {table_name} ROWS 1 TO 1")  # Pega cabeçalhos
            columns = [desc[0].strip() for desc in cursor.description]

        # Cria a tabela no destino se necessário
        self.create_table_in_postgres(table_name, columns)

        # Obtém quantas linhas já foram carregadas no destino
        last_processed_row_count = self.get_destination_row_count(table_name)
        offset = last_processed_row_count

        print(f"Iniciando a migração da tabela {table_name}: {total_records} linhas na origem, {offset} já carregadas.")

        with tqdm(total=total_records - offset, desc=f"Carregando dados de {table_name}") as pbar:
            while offset < total_records:
                with firebirdsql.connect(**self.firebird_config) as conn:
                    cursor = conn.cursor()
                    # Usa a cláusula ROWS para extrair do offset+1 até offset+block_size
                    paginated_query = f"SELECT * FROM {table_name} ROWS {offset + 1} TO {offset + block_size}"
                    cursor.execute(paginated_query)
                    rows = cursor.fetchall()

                    if not rows:
                        break

                    df = pd.DataFrame(rows, columns=columns)
                    self.load_data_using_copy(df, table_name)

                    offset += len(rows)
                    pbar.update(len(rows))

        print(f"Migração da tabela {table_name} finalizada. Total de linhas processadas: {offset}.")

    def run_migration(self, tables_to_load):
        available_tables = self.list_firebird_tables()
        # Considera somente as tabelas que existem na origem
        tables_to_load = [table for table in tables_to_load if table in available_tables]

        print("Tabelas disponíveis no Firebird:", available_tables)
        print("Tabelas a serem carregadas:", tables_to_load)

        for table in tables_to_load:
            print(f"Iniciando a migração dos dados da tabela {table} para o PostgreSQL...")
            self.extract_and_load_data_in_chunks(table)
            print(f"Tabela {table} migrada com sucesso!")

if __name__ == "__main__":
    migration = DatabaseMigration()
    # Lista de tabelas a serem migradas
    tables_to_load = [
        "FC11000",
        "FC11100",
        "FC01000",
        "FC03110",
        "FC03000",
        "FC03140",
        "FC03160",
        "FC03100",
        "FC12100",
        "FC02000",
        "FC02200",
        "FC07000",
        "FC07100",
        "FC07200",
        "FC03100",
        "FC15000",
        "FC15100",
        "FC15110",
        "FC03140",
        "FC03110",
        "FC03160",
        "FC03190",
        "FC12100",
        "FC14000",
        "FC14100",
        "FC12110",
        "FC03J10"
    ]
    migration.run_migration(tables_to_load)
    logging.info("Processo de migração finalizado com sucesso.")




    #12001- animais
    #12002- raça de animais
    #12100-Formula das Receitas
    #12111-vazio
#F0D000-Laboratório
#F0D100-laboratório especies
    #FC0H100-VOLUME DE CAPSULAS POR TIPO
    #FC01A00-MONITORAMENTO DE PESAGEM
    #FC01000-FILIAIS
    #FC02000-FORNECEDORES
    #FC02200-CATALOGO DE PRODUTOS POR FORNECEDOR
    #FC02300-OBSERVAÇÕES/OCORRENCIA DE FORNECEDORES
    #FC03G00-EMBALAGEM/ROTULO POR PRODUTO E ESPECIE
    #FC03N00-
    #FC03000-PRODUTOS
    #FC03100-ESTOQUE DE PRODUTOS POR FILIAL
    #FC03110-DETALHAMENTO DE ENTRADA E SAIDA POR MES
    #FC03140-ESTOQUE DE PRODUTOS POR LOTE
    #FC03160-historico de preços produto
    #FC03190-controle de fechamento do mes
    #FC03200-sinonimos
    #FC03300-INCOMPATIBILIDADES
    #FC03600-PRODUTO associado
    #FC04000-medicos
    #FC04300-ESPECIALIDADES dos medicos
    #FC05000-formulas
    #FC05900-filtro de formulas
    #FC05910- itens dos filtros de formulas
    # "FC07J00" -
    # "FC07000",-clientes
    # "FC07100",-acumulado de receitas e varejo dos clientes por mes
    # "FC07200", -endereço dos clientes
    # "FC07700", -requisiçoes originais
    # "FC07710", -requsiiçoes originadas
    # "FC1B000", -cotacoes
    # "FC1B100", -cotacoes produtos necessarios
    #"FC1B200", -cotacoes fornecedores
    #"FC1B300", -cotacoes fornecedores produtos
    # "FC1E000", -nota fiscal eletronica
    # "FC99W00", -
    # "FC11000", -notas fiscais de entrada
    # "FC11100", -itens das notas fiscais
    # "FC11200", -duplicatas das notas fiscais
    # "FC12000", -receitas
    # "FC12110", -componentes das formulas das receitas
    #"FC12530", -operaçoes da produção
    #"FC12540", - etapas da produção
    #"FC12900", -historio preço das formulas
    #"FC13000", -transferencias
    #"FC13100", -itens das transferencias
    #"FC14000", -varejo
    #"FC14100", -itens do varejo
    #"FC15000", -orcamentos
    #"FC15100", -formula dos orçamentos
    #"FC15120", -vazio
    #FC15110- itens dos orçamentos
    #"FC15800", -
    #"FC16000", -pedidos
    #"FC16100", -itens dos pedidos
    #"FC17000", -requisiçoes
    #"FC17100", -recebimento de requisiçoes
    #"FC17200", -funcionarios que atuaram nas requisições
    #"FC18000", -ORDENS DE PRODUÇÃO DE FORMULA PADRÃO
    #"FC18100", -ITENS DAS ORDENS DE PRODUÇÃO
    #"FC31100", -CAIXA VENDA
    #"FC31110", -CAIXA ITENS DAS VENDAS
    #"FC31111", -CAIXA LOTES POR ITENS
    #"FC31120", -CAIXA DUPLICATA
    #"FC31200", -CAIXA DETALHAMENTO DE VENDAS DE REQUISIÇÕES
    #"FC32100", -CAIXA PEDIDOS VENDAS
    #"FC32110", -CAIXA PEDIDOS ITENS DAS VENDAS
    #"FC32200", -CAIXA PEDIDO -DETALHAMENTO DE VENDAS DE REQUSIISÇÕES
    #"FC41000", -FLUXO DE CAIXA
    #"FC42000", -CONTA CORRENTE
    #"FC42100", -CONTA CORRENTE EXTRATO
    #"FC99999", -PARAMETROS
    #"FC08000", -FUNCIONARIOS
    #"FC04200", -VISITADORES DOS MEDICOS
    #"FC04400", -ENDEREÇO DOS MEDICOS
    #"FC01500" -
















