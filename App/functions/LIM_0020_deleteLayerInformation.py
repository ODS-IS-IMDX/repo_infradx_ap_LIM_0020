# © 2026 NTT DATA Japan Co., Ltd. & NTT InfraNet All Rights Reserved.

"""
LIM_0020_deleteLayerInformation.py

処理名:
    レイヤ情報削除

概要:
    指定されたレイヤIDをもとに、各種マスタのレコード、および紐づく設備データ管理マスタDB、最終断面テーブル、GeoServer定義を削除する。
    削除対象は以下の通りとする。
    ・指定されたレイヤIDに紐づくGeoServerのレイヤ定義
    ・指定されたレイヤIDに紐づく設備データ管理マスタDB、最終断面テーブル
    ・指定されたレイヤIDに紐づくベクタレイヤマスタデータ
    ・ベクタレイヤマスタと外部キー制約で紐づく認可レイヤ（設備）データ

実行コマンド形式:
    python3 [バッチ格納先パス]/LIM_0020_deleteLayerInformation.py
    --provider_code=[公益事業者・道路管理者コード] --layer_id=[レイヤID（複数）]
"""

import argparse
import re
import traceback

from core.config_reader import read_config
from core.constants import Constants
from core.database import Database
from core.geoserverRequest import GeoServerRequest
from core.logger import LogManager
from core.secretProperties import SecretPropertiesSingleton
from core.validations import Validations

log_manager = LogManager()
logger = log_manager.get_logger("LIM_0020_レイヤ情報削除")
config = read_config(logger)

CODE_LIST = {
    "provider_code": "公益事業者・道路管理者コード",
    "layer_id": "レイヤID",
}


# 起動パラメータを受け取る関数
def parse_args():
    try:
        # 完全一致のみ許可
        parser = argparse.ArgumentParser(allow_abbrev=False, exit_on_error=False)
        parser.add_argument("--provider_code", required=False)
        parser.add_argument("--layer_id", required=False)
        return parser.parse_args()
    except Exception as e:
        # コマンドライン引数の解析に失敗した場合
        logger.error("BPE0037", str(e.message))
        logger.process_error_end()


# 1.入力値チェック
def validate_inputs(param):

    # パラメータを展開
    (
        provider_code,
        layer_ids,
    ) = param

    # チェック項目：公益事業者・道路管理者コード
    # 半角数字とハイフンのみで構成されているか
    if not re.match(r"^[0-9-]+$", provider_code):
        logger.error("BPE0019", "公益事業者・道路管理者コード", provider_code)
        logger.process_error_end()

    # 桁数（1以上20以下）であるか
    if not Validations.is_valid_length(provider_code, 1, 20):
        logger.error("BPE0019", "公益事業者・道路管理者コード", provider_code)
        logger.process_error_end()

    # チェック項目：レイヤID（複数）
    # チェックOKのレイヤIDリスト
    valid_layer_ids = []
    # チェックNGのレイヤIDリスト
    invalid_layer_ids = []

    # レイヤID数分繰り返し
    for layer_id in layer_ids:
        if (
            #  半角英数字とアンダースコアのみで構成されているか
            not Validations.is_alnum_underscore(layer_id)
            # フォーマットチェック（正規表現 = r'^[a-z0-9_]+$'）
            or not re.match(r"^[a-z0-9_]+$", layer_id)
            # 桁数（1以上50以下）
            or not Validations.is_valid_length(layer_id, 1, 50)
        ):
            # チェックNGの場合、チェックNGのレイヤIDリストに追加し次のレイヤIDへ
            invalid_layer_ids.append(layer_id)
            continue

        # レイヤIDリストの最初のレイヤID末尾の公益事業者・道路管理者IDを取得
        first_suffix = layer_ids[0].split("_")[-1]
        # すべてのレイヤID末尾の公益事業者・道路管理者IDが同じ値であるか
        if layer_id.split("_")[-1] != first_suffix:
            logger.error("BPE0019", "レイヤID", ", ".join(layer_ids))
            logger.process_error_end()

        # チェックOKの場合、チェックOKのレイヤIDリストに追加
        valid_layer_ids.append(layer_id)

    # チェックNGのレイヤIDが1件以上ある場合、ログに出力
    if invalid_layer_ids:
        invalid_list_str = ",".join(invalid_layer_ids)
        logger.warning(
            "BPW0003",
            "レイヤID",
            invalid_list_str,
        )
    elif not valid_layer_ids:
        logger.warning(
            "BPW0003",
            "レイヤID",
            layer_ids,
        )
    return valid_layer_ids


# 2. 公益事業者・道路管理者存在確認
def check_provider_exists(conn, db_mst_schema, provider_code, layer_id):
    # レイヤIDリストの最初のレイヤID末尾の公益事業者・道路管理者IDを取得
    first_suffix = layer_id.split("_")[-1]
    # 2-1. 公益事業者・道路管理者マスタとの整合性チェック
    query = (
        f"SELECT EXISTS (SELECT 1 FROM {db_mst_schema}.mst_provider "
        "WHERE provider_code = %s"
        "AND provider_id = %s)"
    )
    result = Database.execute_query(
        conn, logger, query, (provider_code, first_suffix), fetchone=True
    )
    if not result:
        logger.error(
            "BPE0057",
            "公益事業者・道路管理者コード",
            provider_code,
            "公益事業者・道路管理者ID",
            first_suffix,
        )
        logger.process_error_end()


# 3. 削除対象ベクタレイヤ存在確認
def check_vector_layer_exists(conn, db_mst_schema, layer_id):
    # ベクタレイヤマスタからレイヤIDに紐づく設備小項目IDを取得
    query = (
        f"SELECT fac_subitem_id FROM {db_mst_schema}.mst_vector_layer "
        "WHERE layer_id = %s"
    )
    try:
        result = Database.execute_query(
            conn,
            logger,
            query,
            (layer_id,),
            fetchone=True,
            raise_exception=True,
        )
    except Exception:
        logger.warning("BPW0010", "ベクタレイヤ存在確認", layer_id)
        raise
    if not result:
        logger.warning("BPW0004", "ベクタレイヤマスタ", layer_id)
        # 削除対象ベクタレイヤが存在しない場合、呼び出し元に例外をキャッチさせて次のレイヤIDへ
        raise Exception
    return result


# 4. 利用終了年月日更新
def update_end_date(conn, db_mst_schema, layer_id):
    # 利用終了年月日を過去日に更新
    query = (
        f"UPDATE {db_mst_schema}.mst_vector_layer SET end_date_of_use = %s "
        "WHERE layer_id = %s"
    )
    try:
        Database.execute_query(
            conn,
            logger,
            query,
            ("19990101", layer_id),
            commit=True,
            raise_exception=True,
        )
    except Exception:
        logger.warning("BPW0010", "利用終了年月日更新", layer_id)
        raise


# 5. レイヤ定義存在確認
def check_geoserver_layer_exists(layer_id):
    # GeoServerのREST APIを使ってレイヤ定義の存在を確認し、HTTPステータスコードを返す
    try:
        status_code = GeoServerRequest.check_layer_exists_common(
            layer_id, logger, raise_exception=True
        )
    except Exception:
        logger.warning("BPW0011", "レイヤ定義存在確認", layer_id)
        raise
    if not status_code == Constants.HTTP_STATUS_OK:
        logger.info("BPI0004", "レイヤ定義", layer_id)
    return status_code


# 6. レイヤ定義削除
def delete_geoserver_layer_definition(layer_id):
    # GeoServerのREST APIを使ってレイヤ定義を削除し、HTTPステータスコードを返す
    try:
        status_code = GeoServerRequest.delete_layer_common(
            layer_id,
            Constants.VECTOR_LAYER_CATEGORY,
            logger,
            raise_exception=True,
        )
    except Exception:
        logger.warning("BPW0011", "レイヤ定義削除", layer_id)
        raise
    if status_code == Constants.HTTP_STATUS_OK:
        logger.info("BPI0005", "レイヤ定義", layer_id)
    else:
        logger.warning("BPW0005", "削除", layer_id)
    return status_code


# 7. 最終断面テーブル存在確認
def check_mv_table_exists(conn, db_mv_schema, layer_id):
    query = (
        "SELECT EXISTS (SELECT 1 FROM pg_matviews"
        " WHERE schemaname = %s AND matviewname = %s)"
    )
    try:
        result = Database.execute_query(
            conn,
            logger,
            query,
            (db_mv_schema, layer_id),
            fetchone=True,
            raise_exception=True,
        )
    except Exception:
        logger.warning("BPW0013", "最終断面テーブル存在確認", layer_id, conn.info.host)
        raise
    if not result:
        logger.info("BPI0012", "最終断面テーブル", layer_id, conn.info.host)

    return result


# 8. 最終断面テーブル削除
def delete_mv_table(mv_conn, db_mv_schema, layer_id):
    query = f"DROP MATERIALIZED VIEW {db_mv_schema}.{layer_id} CASCADE"
    try:
        Database.execute_query(
            mv_conn,
            logger,
            query,
            commit=True,
            raise_exception=True,
        )
    except Exception:
        logger.warning("BPW0013", "最終断面テーブル削除", layer_id, mv_conn.info.host)
        raise
    logger.info("BPI0013", "最終断面テーブル", layer_id, mv_conn.info.host)


# 9. 設備データ管理マスタDB削除可否確認
def is_drop_facility_table(conn, db_mst_schema, fac_subitem_id, layer_id):
    # レイヤIDから公益事業者・道路管理者IDを抽出
    provider_id = layer_id.split("_")[-1]

    query = (
        f"SELECT EXISTS (SELECT 1 FROM {db_mst_schema}.mst_vector_layer "
        "WHERE fac_subitem_id = %s AND provider_id = %s AND NOT layer_id = %s)"
    )
    try:
        result = Database.execute_query(
            conn,
            logger,
            query,
            (fac_subitem_id, provider_id, layer_id),
            fetchone=True,
            raise_exception=True,
        )
    except Exception:
        logger.warning("BPW0010", "設備データ管理マスタDB削除可否確認", layer_id)
        raise
    if result:
        logger.info("BPI0010", layer_id)
    return result


# 設備テーブル名作成
def create_fac_table_name(layer_id):
    # レイヤIDから設備小項目英名を抽出
    layer_id_parts = layer_id.split("_")
    facility_prefix = (
        f"{'_'.join(layer_id_parts[:-3])}_" if len(layer_id_parts) > 3 else ""
    )
    # レイヤIDから公益事業者・道路管理者IDを抽出
    provider_id = layer_id.split("_")[-1]

    # 「data_[設備小項目英名]_[公益事業者・道路管理者ID]」
    fac_table = "data_" + facility_prefix + provider_id
    return fac_table


# 10. 設備テーブル存在確認
def has_admin_code(conn, db_fac_schema, fac_table, layer_id):
    query = (
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_name = %s AND table_schema = %s)"
    )
    try:
        result = Database.execute_query(
            conn,
            logger,
            query,
            (fac_table, db_fac_schema),
            fetchone=True,
            raise_exception=True,
        )
    except Exception:
        logger.warning("BPW0010", "設備テーブル存在確認", layer_id)
        raise
    if not result:
        logger.info("BPI0004", "設備テーブル", layer_id)

    return result


# 11. 設備テーブル削除
def drop_facility_table(conn, db_fac_schema, fac_table, layer_id):
    # 削除対象の設備テーブルを削除（DROP）する
    query = f"DROP TABLE {db_fac_schema}.{fac_table} CASCADE"
    try:
        Database.execute_query(
            conn,
            logger,
            query,
            commit=True,
            raise_exception=True,
        )
    except Exception:
        logger.warning("BPW0010", "設備テーブル削除", layer_id)
        raise
    logger.info("BPI0005", "設備テーブル", layer_id)


# 12-1. レイヤ名取得
def get_layer_id_name(conn, db_mst_schema, layer_id):
    query = (
        f"SELECT layer_name "
        f"FROM {db_mst_schema}.mst_vector_layer "
        "WHERE layer_id = %s"
    )
    try:
        result = Database.execute_query(
            conn,
            logger,
            query,
            (layer_id,),
            fetchone=True,
            raise_exception=True,
        )
    except Exception:
        logger.warning("BPW0010", "ベクタレイヤマスタデータ削除", layer_id)
        raise
    return result


# 12-2. ベクタレイヤマスタデータ削除
def delete_vector_layer_data(conn, db_mst_schema, layer_id, layer_id_name):
    # ベクタレイヤマスタから削除対象のデータを削除
    query = f"DELETE FROM {db_mst_schema}.mst_vector_layer WHERE layer_id = %s"
    try:
        Database.execute_query(
            conn, logger, query, (layer_id,), commit=True, raise_exception=True
        )
    except Exception:
        logger.warning("BPW0010", "ベクタレイヤマスタデータ削除", layer_id)
        raise
    logger.info("BPI0019", "ベクタレイヤマスタデータ", layer_id, layer_id_name)


# 14. 終了コード返却
def determine_exit_code(request_layer_ids_count, deleted_layer_ids_count):
    # リクエストされたレイヤID数 = ベクタレイヤマスタから削除できたレイヤID数の場合
    if request_layer_ids_count == deleted_layer_ids_count:
        # 正常終了
        logger.process_normal_end()
    # ベクタレイヤマスタから削除できたレイヤID数 = 0の場合
    elif deleted_layer_ids_count == 0:
        # 異常終了
        logger.process_error_end()
    # リクエストされたレイヤID数 > ベクタレイヤマスタから削除できたレイヤID数の場合
    elif request_layer_ids_count > deleted_layer_ids_count:
        # 警告終了
        logger.process_warning_end()


# メイン処理
# ベクタレイヤマスタ削除
def main():

    try:
        # 開始ログ出力
        logger.process_start()

        # 起動パラメータの取得
        args = parse_args()

        param = [
            args.provider_code,
            args.layer_id,
        ]

        # 1. 入力値チェック
        # 必須チェック
        for value, key in zip(param, CODE_LIST.keys()):
            if not value:
                logger.error("BPE0018", CODE_LIST[key])
                logger.process_error_end()

        # カンマ区切りのレイヤIDを分割してリストに変換
        layer_ids = param[1].split(",")
        param[1] = layer_ids

        # レイヤIDの項目チェック
        valid_layer_ids = validate_inputs(param)

        # リクエストされたレイヤID数
        request_layer_ids_count = len(layer_ids)
        # ベクタレイヤマスタから削除できたレイヤID数
        deleted_layer_ids_count = 0

        # チェックOKのレイヤIDが0件の場合は異常終了
        if not valid_layer_ids:
            determine_exit_code(request_layer_ids_count, deleted_layer_ids_count)

        # secret_nameをconfigから取得し、secret_propsにAWS Secrets Managerの値を格納
        secret_name = config["aws"]["secret_name"]
        secret_props = SecretPropertiesSingleton(secret_name, config, logger)

        # シークレットからマスタ管理スキーマ名を取得
        db_mst_schema = secret_props.get("db_mst_schema")
        db_fac_schema = secret_props.get("db_fac_schema")
        db_mv_hosts = [
            host.strip()
            for host in secret_props.get("db_mv_host").split(",")
            if host.strip()
        ]

        # DB接続を取得
        conn = Database.get_mstdb_connection(logger)

        # 2. 公益事業者・道路管理者存在確認
        check_provider_exists(conn, db_mst_schema, param[0], valid_layer_ids[0])

        # 入力値チェックを通過したレイヤIDの数分繰り返し
        for layer_id in valid_layer_ids:

            try:
                # 3. 削除対象ベクタレイヤ存在確認
                fac_subitem_id = check_vector_layer_exists(
                    conn, db_mst_schema, layer_id
                )

                # 4. 利用終了年月日更新
                update_end_date(conn, db_mst_schema, layer_id)

                # 5. レイヤ定義存在確認
                exists_status_code = check_geoserver_layer_exists(layer_id)
                # レイヤ定義ありの場合6.を実行
                if exists_status_code == Constants.HTTP_STATUS_OK:
                    # 6. レイヤ定義削除
                    delete_status_code = delete_geoserver_layer_definition(layer_id)
                    # レイヤ定義の削除に失敗した場合は次のレイヤIDへ
                    if not delete_status_code == Constants.HTTP_STATUS_OK:
                        continue

                # 7. 最終断面テーブル存在確認
                for mv_host in db_mv_hosts:
                    if "_2d_" in layer_id:
                        db_mv_schema = secret_props.get("db_mv_2d_schema")
                    else:
                        db_mv_schema = secret_props.get("db_mv_3d_schema")
                    with Database.get_refdb_connection(mv_host, logger):
                        if check_mv_table_exists(conn, db_mv_schema, layer_id):
                            # 8. 最終断面テーブル削除
                            delete_mv_table(conn, db_mv_schema, layer_id)

                # 9. 設備データ管理マスタDB削除可否確認
                if not is_drop_facility_table(
                    conn, db_mst_schema, fac_subitem_id, layer_id
                ):
                    # 10. 設備テーブル存在確認
                    # 設備テーブル名作成
                    fac_table = create_fac_table_name(layer_id)
                    if has_admin_code(conn, db_fac_schema, fac_table, layer_id):
                        # 11. 設備テーブル削除
                        drop_facility_table(conn, db_fac_schema, fac_table, layer_id)

                # 12. ベクタレイヤマスタデータ削除
                # 12-1. レイヤ名取得
                layer_id_name = get_layer_id_name(conn, db_mst_schema, layer_id)
                # 12-2. ベクタレイヤマスタデータ削除
                delete_vector_layer_data(conn, db_mst_schema, layer_id, layer_id_name)

                # 13. 削除数カウント
                deleted_layer_ids_count += 1

            # ループ処理の中で例外が発生した場合は次のレイヤIDへ
            except Exception:
                continue

        # 14. 終了コード返却
        determine_exit_code(request_layer_ids_count, deleted_layer_ids_count)

    except Exception:
        logger.error("BPE0009", traceback.format_exc())
        logger.process_error_end()


if __name__ == "__main__":
    main()
