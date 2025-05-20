import os
import io
import firebirdsql
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from apscheduler.schedulers.blocking import BlockingScheduler
from tqdm import tqdm
import logging

class DatabaseMigration:
    def __init__(self):
        load_dotenv()

        self.firebird_config = {
            'host': os.getenv("FIREBIRD_HOST"),
            'database': os.getenv("FIREBIRD_DATABASE"),
            'user': os.getenv("FIREBIRD_USER"),
            'password': os.getenv("FIREBIRD_PASSWORD"),
            'charset': 'ISO8859_1'
        }

        self.engine = create_engine(
            f'postgresql+psycopg2://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}/{os.getenv("POSTGRES_DB")}',
            pool_size=5, max_overflow=10
        )

        self.ensure_bronze_schema()

        logging.basicConfig(filename='migration.log', level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

    def ensure_bronze_schema(self):
        with self.engine.connect() as connection:
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
            connection.commit()

    def create_table_in_postgres(self, table_name, columns):
        column_definitions = ", ".join([f'"{col}" TEXT' for col in columns])
        query = f'CREATE TABLE IF NOT EXISTS bronze."{table_name}" ({column_definitions});'
        with self.engine.connect() as connection:
            connection.execute(text(query))
            connection.commit()

    def list_firebird_tables(self):
        with firebirdsql.connect(**self.firebird_config) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 0;")
            return [row[0].strip() for row in cursor.fetchall()]

    def sanitize_dataframe(self, df):
        return df.apply(lambda col: col.map(lambda x: x.replace('\x00', '') if isinstance(x, str) else x))

    def load_data_using_copy(self, df, table_name):
        df = self.sanitize_dataframe(df)
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
        with self.engine.connect() as connection:
            result = connection.execute(text(f'SELECT COUNT(*) FROM bronze."{table_name}"'))
            return result.scalar()

    def extract_and_load_data_in_chunks(self, table_name, block_size=10000):
        with firebirdsql.connect(**self.firebird_config) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_records = cursor.fetchone()[0]

            cursor.execute(f"SELECT * FROM {table_name} ROWS 1 TO 1")
            columns = [desc[0].strip() for desc in cursor.description]

        self.create_table_in_postgres(table_name, columns)
        offset = self.get_destination_row_count(table_name)

        print(f"Iniciando a migração da tabela {table_name}: {total_records} linhas na origem, {offset} já carregadas.")

        with tqdm(total=total_records - offset, desc=f"Carregando {table_name}") as pbar:
            while offset < total_records:
                with firebirdsql.connect(**self.firebird_config) as conn:
                    cursor = conn.cursor()
                    query = f"SELECT * FROM {table_name} ROWS {offset + 1} TO {offset + block_size}"
                    cursor.execute(query)
                    rows = cursor.fetchall()

                if not rows:
                    break

                df = pd.DataFrame(rows, columns=columns)
                self.load_data_using_copy(df, table_name)
                offset += len(rows)
                pbar.update(len(rows))

    def run_migration(self):
        tables_to_load = [
            "FC11000", "FC11100", "FC31110", "FC31100", "FC03000", "FC04200", "FC04300", 
            "FC0D100", "FC01000", "FC03110",
            "FC03140", "FC03160", "FC03100", "FC12100", "FC17000", "FC02000", "FC02200", "FC31200",
            "FC07000", "FC07100", "FC07200", "FC15000", "FC15100", "FC15110", "FC03190",
            "FC14000", "FC14100", "FC12110", "FC12400", "FC12440", "FC12442", "FC04000",
            "FC08000", "FC06000", "FC12410","FC0D000", "FC1B100","FC12500"
        ]

        available_tables = self.list_firebird_tables()
        valid_tables = [t for t in tables_to_load if t in available_tables]

        for table in valid_tables:
            try:
                print(f"Iniciando migração da tabela {table}")
                self.extract_and_load_data_in_chunks(table)
                logging.info(f"Tabela {table} migrada com sucesso.")
            except Exception as e:
                logging.error(f"Erro ao migrar a tabela {table}: {e}")

if __name__ == "__main__":
    print("Executando migração ")
    migrator = DatabaseMigration()
    migrator.run_migration()




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
















