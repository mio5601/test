/*
 * PICOCOM PROPRIETARY INFORMATION
 *
 * This software is supplied under the terms of a license agreement or
 * nondisclosure agreement with PICOCOM and may not be copied
 * or disclosed except in accordance with the terms of that agreement.
 *
 * Copyright PICOCOM.
 */

#include "api_message_table.h"
#include "board_type.h"
#include "file_transfer.h"
#include "file_utils.h"
#include "hardware_mgmt.h"
#include "i2c_message_manager.h"
#include "i2c_utils.h"
#include "internal_pipe.h"
#include "log.h"
#include "measurement_counter_definitions.h"
#include "measurement_counter_handler.h"
#include "netconf.h"
#include "oam_config_parser.h"
#include "oru_oam_api.h"
#include "platform.h"
#include "sfp_manager.h"
#include "sftp_transfer.h"
#include "time_utils.h"
#include "uri_parser.h"
#include "xml_parser.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define BASIC_NOTIF_ITEM_CNT       3
#define TX_RX_WIN_NOTIF_ITEM_CNT   5
#define TRANSCEIVER_NOTIF_ITEM_CNT 11

#define USED_MEAS_GRP_CNT 4
#define MAX_MEAS_OBJ_CNT  9
#define MEAS_OBJ_MAP      0b111111111

#define FILE_UPLOAD        1
#define RANDOM_FILE_UPLOAD 2

#define ENVIRONMENTAL_SAMPLE_INTERVAL 5
#define ENERGY_POWER_SAMPLE_INTERVAL  1

#define AVG_VALUE(result, last_total, counter) ((result + last_total) / (counter))

#define RESULT_STR_LEN 32

#define RX_WINDOW_MEASUREMNT_FORMAT_STR \
    "%u,%s,%s%s,%s%s,%s,%" PRIu64 "\n" /* The format meaning is: measurement group id, measurement object, start time, end time, name of RU, counter result */
#define TX_MEASUREMNT_FORMAT_STR RX_WINDOW_MEASUREMNT_FORMAT_STR

#define TRX_POWER_NOTATION     1000
#define TRX_POWER_CONVERSION   10
#define TRX_CURRENT_NOTATION   1000
#define TRX_CURRENT_CONVERSION 2

#define MAX_XPATH_LENGTH 256

typedef enum {
    TEMPERATURE = 0x00,
    POWER,
    VOLTAGE,
    CURRENT,
    EPE_MEASUREMENT_OBJECT_NUM,
} epe_measurement_object_e;

typedef enum {
    EPE_MEASUREMENT_REPORT_TYPE_MAXIMUM = 0x00,
    EPE_MEASUREMENT_REPORT_TYPE_MINIMUM,
    EPE_MEASUREMENT_REPORT_TYPE_AVERAGE,
    EPE_MEASUREMENT_REPORT_TYPE_NUM,
} epe_measurement_report_type_e;

typedef enum {
    TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE = 0x00,
    TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE,
    TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT,
    TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER,
    TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER,
    TRANSCEIVER_MEASUREMENT_OBJECT_NUM,
} transceiver_measurement_object_e;

typedef struct perf_mgmt_notif_data {
    sr_val_t* values;
    struct perf_mgmt_notif_data* next;
} perf_mgmt_notif_data_t;

typedef struct {
    uint32_t data_cnt;
    perf_mgmt_notif_data_t* data_head;
    perf_mgmt_notif_data_t* data_end;
} perf_mgmt_measurement_stats_t;

typedef struct {
    uint8_t active;
    /** Used to ensure the index of key-less list in sysrepo
     *  start from 1 and increases one by one continuously. */
    uint32_t index;
    uint8_t data_cnt;
    sr_val_t* data;
} perf_mgmt_latest_notif_t;

typedef struct {
    pthread_t tid;
    timer_t timerid;
    uint16_t interval;
    pthread_mutex_t notif_mutex;
    perf_mgmt_measurement_stats_t measurement_stats;
    perf_mgmt_latest_notif_t latest_notif[USED_MEAS_GRP_CNT][MAX_MEAS_OBJ_CNT];
} perf_mgmt_notif_ctx_t;

typedef struct {
    pthread_t file_upload_tid;
    timer_t file_upload_timerid;
    uint16_t file_upload_interval;
    uint8_t file_upload_mode; // 0:disable file upload, 1: enable file upload, 2: enable SFP file upload, 3: enable random file upload
} perf_mgmt_file_upload_ctx_t;

typedef struct {
    char* sensor_name;
    sensor_type_e sensor_type;
    uint64_t counter;
    uint8_t active_map;
    float monitor_result[EPE_MEASUREMENT_OBJECT_NUM][EPE_MEASUREMENT_REPORT_TYPE_NUM];
} epe_measurement_result_t;

typedef struct {
    pthread_t tid;
    uint16_t interval;
    timer_t energy_power_sample_timerid;
    timer_t environmental_sample_timerid;
    uint32_t epe_result_list_num;
    epe_measurement_result_t* epe_result_list;
    uint16_t temperature_sensor_num;
    uint16_t power_rail_sensor_num;
} perf_mgmt_epe_measurement_ctx_t;

typedef struct {
    /* diagnostic monitor type */
    bool enabled;
    bool internal_calibrated;
    bool external_calibrated;

    /* external calibration consts */
    float rx_power_calibration[5];
    uint16_t I_slope;
    int16_t I_offset;
    uint16_t tx_power_slope;
    int16_t tx_power_offset;
    uint16_t t_slope;
    int16_t t_offset;
    uint16_t v_slope;
    int16_t v_offset;
} transceiver_diagnostic_monitor_info_t;

typedef struct {
    int16_t temperature;
    uint16_t voltage;
    uint16_t tx_bias_current;
    uint16_t tx_power;
    uint16_t rx_power;
} transceiver_realtime_data_t;

typedef struct {
    uint16_t min;
    uint16_t max;
    uint16_t first;
    uint16_t latest;
    char min_time[LOCAL_TIME_STR_LEN];
    char max_time[LOCAL_TIME_STR_LEN];
    char first_time[LOCAL_TIME_STR_LEN];
    char latest_time[LOCAL_TIME_STR_LEN];
} transceiver_report_info_t;

typedef struct {
    /* indicate whether this measurement-object-id is active */
    bool active;
    bool min_active;
    bool max_active;
    bool first_active;
    bool latest_active;
    double min;
    double max;
    double first;
    double latest;
    char min_time[LOCAL_TIME_STR_LEN];
    char max_time[LOCAL_TIME_STR_LEN];
    char first_time[LOCAL_TIME_STR_LEN];
    char latest_time[LOCAL_TIME_STR_LEN];
    uint16_t bin_count;
    double lower_bound;
    double upper_bound;
    uint32_t* freq_bin_table;
} transceiver_result_info_t;

typedef struct {
    pthread_t tid;
    uint16_t interval;
    bool is_first_record_set;
    transceiver_diagnostic_monitor_info_t diag_monitor_info;
    transceiver_realtime_data_t rt_meas;
    transceiver_report_info_t record[TRANSCEIVER_MEASUREMENT_OBJECT_NUM];
    transceiver_result_info_t result[TRANSCEIVER_MEASUREMENT_OBJECT_NUM];
} perf_mgmt_transceiver_measurement_ctx_t;

typedef struct {
    measurement_group_e measurement_group;
    uint16_t measurement_interval;
    union {
        measurement_counter_result_ind_t* counter_result; /* The result of tx/rx_windows measurement counter from the cluster. */
        epe_measurement_result_t* epe_result;             /* The result of epe measurement result from sensors. */
        transceiver_result_info_t* transceiver_result;    /* The result of transceiver measurement result from SFP. */
    };
} perf_mgmt_measurement_result_t;

typedef struct {
    char* radio_unit_name;
    char* radio_unit_name_with_serial_num;
    FILE* csv_fp; // The current csv file pointer.
    pthread_mutex_t file_mutex;
    char curr_csv_file_path[MAX_CSV_FILE_PATH_LEN];
    hal_config_t* hal_cfg; // Manage all sensor.
    perf_mgmt_notif_ctx_t notif_ctx;
    perf_mgmt_file_upload_ctx_t file_upload_ctx;
    perf_mgmt_epe_measurement_ctx_t epe_measurement_ctx;
    perf_mgmt_transceiver_measurement_ctx_t transceiver_meas_ctx;
} oam_perf_mgmt_ctx_t;

/********************************************<global variables declaration>*************************************************/

static oam_perf_mgmt_ctx_t gPerfMgmtCtx = { 0 };

static char* gSupportMeasGroupName[] = {
    "tx",
    "rx-window",
    "epe",
    "transceiver",
};

static char* get_meas_obj_name(measurement_group_e meas_grp, uint32_t meas_obj_id);

static char* get_epe_measurement_result_leaf_str(uint32_t epe_measurement_report);

/********************************************<function definition>*************************************************/

static oam_perf_mgmt_ctx_t* get_oam_perf_mgmt_ctx()
{
    return &gPerfMgmtCtx;
}

static void perf_mgmt_set_curr_file(oam_perf_mgmt_ctx_t* perf_mgmt_ctx, char* perf_file_path)
{
    char start_time_str[LOCAL_TIME_STR_LEN];
    char end_time_str[LOCAL_TIME_STR_LEN];
    char time_zone[TIME_ZONE_STR_LEN];
    int rc = 0;
    time_t now = time(NULL);
    time_t start_time = now;
    time_t end_time = now + perf_mgmt_ctx->file_upload_ctx.file_upload_interval;

    rc = get_local_timezone(FILE_NAME_TIME_ZONE_FORMAT, time_zone);
    if (rc != 0) {
        OAM_LOG_ERROR("failed to get time zone");
        return;
    }

    strftime(start_time_str, sizeof(start_time_str), FILE_CONTENT_LOCAL_TIME_FORMAT, localtime(&start_time));
    strftime(end_time_str, sizeof(end_time_str), FILE_CONTENT_LOCAL_TIME_FORMAT, localtime(&end_time));
    snprintf(perf_mgmt_ctx->curr_csv_file_path, sizeof(perf_mgmt_ctx->curr_csv_file_path), "%s/C%s%s_%s%s_%s.csv", perf_file_path, start_time_str, time_zone, end_time_str,
        time_zone, perf_mgmt_ctx->radio_unit_name_with_serial_num);

    // close last csv file
    if (perf_mgmt_ctx->csv_fp) {
        fclose(perf_mgmt_ctx->csv_fp);
    }

    // open new csv file
    perf_mgmt_ctx->csv_fp = fopen(perf_mgmt_ctx->curr_csv_file_path, "a");
    if (!perf_mgmt_ctx->csv_fp) {
        OAM_LOG_ERROR("failed to open file: [%s]", perf_mgmt_ctx->curr_csv_file_path);
        return;
    }

    remove_oldest_file(perf_file_path, MAX_CSV_FILE_NUM, ".csv");
}

static int perf_mgmt_common_file_upload(char* file_path, const char* list_name, const char* leaf_name, char* root_path)
int rc = SR_ERR_OK;
{
    char* ret = NULL;
    char xpath[XPATH_LEN];
    sr_val_t* val_auth = NULL;
    sr_val_t* values = NULL;
    size_t values_count = 0;
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* session = oam_netconf_ctx->netconf_ds_sess_running;
    char root_xpath[] = "/o-ran-performance-management:performance-measurement-objects";

    snprintf(xpath, sizeof(xpath), "%s/%s/%s", root_xpath, list_name, leaf_name);
    rc = sr_get_items(session, xpath, 0, 0, &values, &values_count);
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the list \"%s\"", leaf_name);
        goto error;
    }

    for (size_t i = 0; i < values_count; i++) {
        if (!strncmp("sftp", values[i].data.string_val, sizeof("sftp") - 1)) {
            file_mgmt_arg_t file_upload_arg = { 0 };
            memset(xpath, 0, sizeof(xpath));
            ret = strrchr(values[i].xpath, '/');
            if (ret == NULL) {
                goto error;
            }
            strncpy(xpath, values[i].xpath, ret - values[i].xpath);
            strcat(xpath, "/password/password");
            rc = sr_get_item(session, xpath, 0, &val_auth);
            if (rc != SR_ERR_OK) {
                char** publickey_algorithm_collection = get_publickey_algorithm_collection();
                size_t public_key_count = get_publickey_algorithm_count();
                char public_key_path[XPATH_LEN];
                int found_public_key = 0;
                for (size_t j = 0; j < public_key_count; j++) {
                    rc = check_unsupported_algorithm(publickey_algorithm_collection[j]);
                    if (rc != SR_ERR_OK) {
                        sr_session_set_error_message(session, "do not support secp192r1 or secp224r1");
                        return rc;
                    }
                    memset(xpath, 0, sizeof(xpath));
                    strncpy(xpath, values[i].xpath, ret - values[i].xpath);
                    memset(public_key_path, 0, sizeof(public_key_path));
                    snprintf(public_key_path, sizeof(public_key_path), "/server/keys[algorithm='o-ran-file-management:%s']/public-key", publickey_algorithm_collection[j]);
                    strcat(xpath, public_key_path);
                    rc = sr_get_item(session, xpath, 0, &val_auth);
                    if (rc == SR_ERR_OK) {

                        // Copy the remote URI and logical path
                        if (strlen(file_path) >= MAX_PATH_FILE_LEN) {
                            OAM_LOG_ERROR("logical file path length exceeds %d", MAX_PATH_FILE_LEN);
                            return SR_ERR_OPERATION_FAILED;
                        }

                        if (strlen(values[i].data.string_val) >= MAX_URI_LEN) {
                            OAM_LOG_ERROR("remote uri length exceeds %d", MAX_URI_LEN);
                            return SR_ERR_OPERATION_FAILED;
                        }

                        strncpy(file_upload_arg.local_physical_path, file_path, MAX_PATH_FILE_LEN - 1);
                        strncpy(file_upload_arg.remote_path_uri, values[i].data.string_val, MAX_URI_LEN - 1);
                        file_upload_arg.protocol_type = PROTOCOL_TYPE_SFTP;
                        file_upload_arg.sftp_auth_arg.auth_type = AUTHENTICATION_TYPE_PUBLICKEY;
                        file_upload_arg.sftp_auth_arg.public_key_auth.algorithm_count = public_key_count;
                        // Assign algorithm and public key
                        if (strlen(publickey_algorithm_collection[j]) >= MAX_ALGORITHM_NAME_LEN) {
                            OAM_LOG_ERROR("algorithm name exceeds %d", MAX_ALGORITHM_NAME_LEN);
                            return SR_ERR_OPERATION_FAILED;
                        }

                        if (strlen(val_auth->data.binary_val) >= MAX_PUBLIC_KEY_LEN) {
                            OAM_LOG_ERROR("public key exceeds %d", MAX_PUBLIC_KEY_LEN);
                            return SR_ERR_OPERATION_FAILED;
                        }

                        strncpy(file_upload_arg.sftp_auth_arg.public_key_auth.algorithms[j].algorithm, publickey_algorithm_collection[j], MAX_ALGORITHM_NAME_LEN - 1);
                        strncpy(file_upload_arg.sftp_auth_arg.public_key_auth.algorithms[j].public_key, val_auth->data.binary_val, MAX_PUBLIC_KEY_LEN - 1);
                        OAM_LOG_INFO("upload local file %s to remote %s with protocol sftp uses public key", file_path, file_upload_arg.remote_path_uri);
                        file_upload(&file_upload_arg, root_path);
                        sr_free_val(val_auth);
                        found_public_key = 1;
                        break;
                    }
                }
                if (!found_public_key) {
                    OAM_LOG_ERROR("failed to get a valid public key and password");
                    goto error;
                }
                continue;
            }
            // Copy the remote URI and logical path
            if (strlen(file_path) >= MAX_PATH_FILE_LEN) {
                OAM_LOG_ERROR("logical file path length exceeds %d", MAX_PATH_FILE_LEN);
                return SR_ERR_OPERATION_FAILED;
            }

            if (strlen(values[i].data.string_val) >= MAX_URI_LEN) {
                OAM_LOG_ERROR("remote uri length exceeds %d", MAX_URI_LEN);
                return SR_ERR_OPERATION_FAILED;
            }

            strncpy(file_upload_arg.local_physical_path, file_path, MAX_PATH_FILE_LEN - 1);
            strncpy(file_upload_arg.remote_path_uri, values[i].data.string_val, MAX_URI_LEN - 1);
            file_upload_arg.protocol_type = PROTOCOL_TYPE_SFTP;
            file_upload_arg.sftp_auth_arg.auth_type = AUTHENTICATION_TYPE_PASSWORD;
            // Check if the password is provided
            if (strlen(val_auth->data.string_val) >= MAX_PASSWORD_LEN) {
                OAM_LOG_ERROR("password length exceeds %d", MAX_PASSWORD_LEN);
                return SR_ERR_OPERATION_FAILED;
            }
            strncpy(file_upload_arg.sftp_auth_arg.passwd_auth.password, val_auth->data.string_val, MAX_PASSWORD_LEN - 1);
            OAM_LOG_INFO("upload local file %s to remote %s with protocol sftp uses password", file_path, file_upload_arg.remote_path_uri);
            file_upload(&file_upload_arg, root_path);
            sr_free_val(val_auth);
        } else {
            file_mgmt_arg_t file_upload_arg = { 0 };
            memset(xpath, 0, sizeof(xpath));
            ret = strrchr(values[i].xpath, '/');
            if (ret == NULL) {
                goto error;
            }
            strncpy(xpath, values[i].xpath, ret - values[i].xpath);
            strcat(xpath, "/application-layer-credential/appl-password");
            rc = sr_get_item(session, xpath, 0, &val_auth);
            if (rc != SR_ERR_OK) {
                OAM_LOG_ERROR("failed to get the appl-password");
                goto error;
            }
            if (strlen(file_path) >= MAX_PATH_FILE_LEN) {
                OAM_LOG_ERROR("logical file path length exceeds %d", MAX_PATH_FILE_LEN);
                return SR_ERR_OPERATION_FAILED;
            }

            if (strlen(values[i].data.string_val) >= MAX_URI_LEN) {
                OAM_LOG_ERROR("remote uri length exceeds %d", MAX_URI_LEN);
                return SR_ERR_OPERATION_FAILED;
            }

            if (strlen(val_auth->data.string_val) >= MAX_PASSWORD_LEN) {
                OAM_LOG_ERROR("password length exceeds %d", MAX_PASSWORD_LEN);
                return SR_ERR_OPERATION_FAILED;
            }

            strncpy(file_upload_arg.local_physical_path, file_path, MAX_PATH_FILE_LEN - 1);
            strncpy(file_upload_arg.remote_path_uri, values[i].data.string_val, MAX_URI_LEN - 1);
            strncpy(file_upload_arg.ftpes_auth_arg.appl_password, val_auth->data.string_val, MAX_PASSWORD_LEN - 1);
            file_upload_arg.protocol_type = PROTOCOL_TYPE_FTPES;
            OAM_LOG_INFO("upload local file %s to remote %s with protocol ftp", file_path, file_upload_arg.remote_path_uri);
            file_upload(&file_upload_arg, root_path);
        }
    }

    sr_free_values(values, values_count);
    return 0;

error:
    sr_free_values(values, values_count);
    return -1;
}

static void perf_mgmt_upload_curr_file(char* file_path, uint8_t file_upload_mode, char* root_path)
{
    if (-1 == access(file_path, F_OK)) {
        OAM_LOG_ERROR("fail to upload file, can't access %s", file_path);
        return;
    }

    if (file_upload_mode == FILE_UPLOAD || file_upload_mode == RANDOM_FILE_UPLOAD) {
        perf_mgmt_common_file_upload(file_path, "remote-file-uploads", "remote-file-upload-path", root_path);
    }

    return;
}

void file_upload_timer_handler(union sigval sv)
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = sv.sival_ptr;
    char* file_path = strdup(perf_mgmt_ctx->curr_csv_file_path);

    char* root_path = get_oran_root_path();
    if (!root_path) {
        return;
    }

    char perf_file_path[CONTENT_SIZE + SUB_FOLDER_LENGTH] = { 0 };
    get_oran_sub_path(perf_file_path, sizeof(perf_file_path), root_path, "pm");

    pthread_mutex_lock(&perf_mgmt_ctx->file_mutex);
    perf_mgmt_set_curr_file(perf_mgmt_ctx, perf_file_path);
    pthread_mutex_unlock(&perf_mgmt_ctx->file_mutex);
    perf_mgmt_upload_curr_file(file_path, perf_mgmt_ctx->file_upload_ctx.file_upload_mode, root_path);
    free(file_path);

    struct itimerspec its = { 0 };
    its.it_value.tv_sec = perf_mgmt_ctx->file_upload_ctx.file_upload_interval;
    its.it_interval.tv_sec = perf_mgmt_ctx->file_upload_ctx.file_upload_interval;
    timer_settime(perf_mgmt_ctx->file_upload_ctx.file_upload_timerid, 0, &its, NULL);
}

static void* perf_mgmt_upload_curr_file_thread(void* arg)
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = (oam_perf_mgmt_ctx_t*)arg;

    // Set up the timer
    struct sigevent sev = { 0 };
    sev.sigev_notify = SIGEV_THREAD;
    sev.sigev_notify_function = &file_upload_timer_handler;
    sev.sigev_notify_attributes = NULL;
    sev.sigev_value.sival_ptr = perf_mgmt_ctx;
    char* root_path = get_oran_root_path();
    if (!root_path) {
        pthread_exit(NULL);
    }

    char perf_file_path[CONTENT_SIZE + SUB_FOLDER_LENGTH] = { 0 };
    get_oran_sub_path(perf_file_path, sizeof(perf_file_path), root_path, "pm");
    perf_mgmt_set_curr_file(perf_mgmt_ctx, perf_file_path);

    timer_t timerid;
    if (timer_create(CLOCK_REALTIME, &sev, &timerid) == -1) {
        OAM_LOG_ERROR("timer_create failed");
        return NULL;
    }

    // Set the timer interval
    struct itimerspec its = { 0 };
    its.it_value.tv_sec = perf_mgmt_ctx->file_upload_ctx.file_upload_interval;
    its.it_interval.tv_sec = perf_mgmt_ctx->file_upload_ctx.file_upload_interval;

    // Start the timer
    if (timer_settime(timerid, 0, &its, NULL) == -1) {
        OAM_LOG_ERROR("timer_settime failed");
        return NULL;
    }

    perf_mgmt_ctx->file_upload_ctx.file_upload_timerid = timerid;

    while (perf_mgmt_ctx->file_upload_ctx.file_upload_mode && perf_mgmt_ctx->file_upload_ctx.file_upload_interval) {
        sleep(1);
    }
    timer_delete(timerid);
    perf_mgmt_ctx->file_upload_ctx.file_upload_tid = 0;
    return NULL;
}

static const char* get_meas_grp_name(measurement_group_e meas_grp)
{
    if (meas_grp > USED_MEAS_GRP_CNT) {
        OAM_LOG_ERROR("can't get measurement group name %d", meas_grp);
        return NULL;
    }
    return gSupportMeasGroupName[meas_grp];
}

static int get_meas_obj_id(const char* meas_obj_name, measurement_group_e meas_grp)  // Get the measurement object id by its name.
{
    char* end_pos = strchr(meas_obj_name, '\'');
    int cmp_length = 0;
    int meas_obj_num = 0;

    if (end_pos) {
        cmp_length = end_pos - meas_obj_name;
    } else {
        cmp_length = strlen(meas_obj_name);
    }

    switch (meas_grp) {
        case TX_MEASUREMENT: {
            meas_obj_num = TX_COUNTER_NUM;
        } break;

        case RX_WINDOW_MEASUREMENT: {
            meas_obj_num = RX_COUNTER_NUM;
        } break;

        case EPE_MEASUREMENT: {
            meas_obj_num = EPE_MEASUREMENT_OBJECT_NUM;
        } break;

        case TRANSCEIVER_MEASUREMENT: {
            meas_obj_num = TRANSCEIVER_MEASUREMENT_OBJECT_NUM;
        } break;

        case SYMBOL_RSSI_MEASUREMENT:
        case NUM_OF_MEASUREMENT:
        default:
            break;
    }

    for (size_t i = 0; i < meas_obj_num; i++) {
        const char* support_meas_obj_name = get_meas_obj_name(meas_grp, i);
        if (!strncmp(meas_obj_name, support_meas_obj_name, cmp_length)) {
            return i;
        }
    }

    OAM_LOG_ERROR("%s is not a support measurement object", meas_obj_name);
    return -1;
}

static char* get_epe_measurement_obj_name(uint32_t meas_obj_id)
{
    static char* gEpeMeasObjName[] = {
        "TEMPERATURE",
        "POWER",
        "VOLTAGE",
        "CURRENT",
    };
    if (meas_obj_id >= sizeof(gEpeMeasObjName) / sizeof(gEpeMeasObjName[0])) {
        return NULL;
    }

    return gEpeMeasObjName[meas_obj_id];
}

static char* get_transceiver_meas_obj_name(uint32_t meas_obj_id)
{
    static char* gTransceiverMeasObjName[] = {
        "TEMPERATURE",
        "VOLTAGE",
        "TX_BIAS_COUNT",
        "TX_POWER",
        "RX_POWER",
    };
    if (meas_obj_id >= sizeof(gTransceiverMeasObjName) / sizeof(gTransceiverMeasObjName[0])) {
        OAM_LOG_ERROR("can't get transceiver measurement obj name %d", meas_obj_id);
        return NULL;
    }

    return gTransceiverMeasObjName[meas_obj_id];
}

static double calc_temperature(transceiver_diagnostic_monitor_info_t* info, uint16_t hex_val)
{
    int16_t signed_val = (int16_t)hex_val;
    double temperature = 0;
    if (info->internal_calibrated) {
        double accuracy = 0.004;
        int8_t high_byte = (signed_val >> 8) & 0xFF;
        uint8_t low_byte = signed_val & 0xFF;
        temperature = high_byte + accuracy * low_byte;
    } else if (info->external_calibrated) {
        temperature = info->t_slope * signed_val + info->t_offset;
    }

    return temperature;
}

static double calc_voltage(transceiver_diagnostic_monitor_info_t* info, uint16_t decimal_val)
{
    double voltage = 0;
    if (info->internal_calibrated) {
        double accuracy = 0.1;
        voltage = ((double)decimal_val) * accuracy;
    } else if (info->external_calibrated) {
        voltage = info->v_slope * decimal_val + info->v_offset;
    }

    return voltage;
}

static double calc_tx_bias_current(transceiver_diagnostic_monitor_info_t* info, uint16_t decimal_val)
{
    double current = 0;
    if (info->internal_calibrated) {
        current = ((double)decimal_val / TRX_CURRENT_CONVERSION) / TRX_CURRENT_NOTATION;
    } else if (info->external_calibrated) {
        current = info->I_slope * decimal_val + info->I_offset;
    }

    return current;
}

static double calc_tx_power(transceiver_diagnostic_monitor_info_t* info, uint16_t decimal_val)
{
    double power = 0;
    if (info->internal_calibrated) {
        power = ((double)decimal_val / TRX_POWER_CONVERSION) / TRX_POWER_NOTATION;
    } else if (info->external_calibrated) {
        power = info->tx_power_slope * decimal_val + info->tx_power_offset;
    }

    return power;
}

static double calc_rx_power(transceiver_diagnostic_monitor_info_t* info, uint16_t decimal_val)
{
    double power = 0;
    if (info->internal_calibrated) {
        power = ((double)decimal_val / TRX_POWER_CONVERSION) / TRX_POWER_NOTATION;
    } else if (info->external_calibrated) {
        // refer to SFF-8472-R12.4 clause 9.3
        for (int i = 4; i >= 0; i--) {
            power += (double)info->rx_power_calibration[i] * pow((double)decimal_val, (double)i);
        }
    }

    return power;
}

typedef double (*calc_transceiver_result_t)(transceiver_diagnostic_monitor_info_t*, uint16_t);

static calc_transceiver_result_t get_transceiver_data_calc_handler(transceiver_measurement_object_e meas_obj_id)
{
    switch (meas_obj_id) {
        case TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE: {
            return calc_temperature;
        }

        case TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE: {
            return calc_voltage;
        }

        case TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT: {
            return calc_tx_bias_current;
        }

        case TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER: {
            return calc_tx_power;
        }

        case TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER: {
            return calc_rx_power;
        }

        case TRANSCEIVER_MEASUREMENT_OBJECT_NUM:
            break;

        default:
            break;
    }

    return NULL;
}

static int get_transceiver_meas_result(perf_mgmt_transceiver_measurement_ctx_t* ctx, uint32_t meas_obj_id)
{
    calc_transceiver_result_t calc_handler = get_transceiver_data_calc_handler(meas_obj_id);
    if (!calc_handler) {
        return 0;
    }
    ctx->result[meas_obj_id].min = calc_handler(&ctx->diag_monitor_info, ctx->record[meas_obj_id].min);
    ctx->result[meas_obj_id].max = calc_handler(&ctx->diag_monitor_info, ctx->record[meas_obj_id].max);
    ctx->result[meas_obj_id].first = calc_handler(&ctx->diag_monitor_info, ctx->record[meas_obj_id].first);
    ctx->result[meas_obj_id].latest = calc_handler(&ctx->diag_monitor_info, ctx->record[meas_obj_id].latest);
    strcpy(ctx->result[meas_obj_id].min_time, ctx->record[meas_obj_id].min_time);
    strcpy(ctx->result[meas_obj_id].max_time, ctx->record[meas_obj_id].max_time);
    strcpy(ctx->result[meas_obj_id].first_time, ctx->record[meas_obj_id].first_time);
    strcpy(ctx->result[meas_obj_id].latest_time, ctx->record[meas_obj_id].latest_time);
    return 0;
}

static char* get_meas_obj_name(measurement_group_e meas_grp, uint32_t meas_obj_id)
{
    switch (meas_grp) {
        case TX_MEASUREMENT:
            return get_tx_meas_obj_name(meas_obj_id);

        case RX_WINDOW_MEASUREMENT:
            return get_rx_win_meas_obj_name(meas_obj_id);

        case EPE_MEASUREMENT:
            return get_epe_measurement_obj_name(meas_obj_id);

        case TRANSCEIVER_MEASUREMENT:
            return get_transceiver_meas_obj_name(meas_obj_id);

        case SYMBOL_RSSI_MEASUREMENT:
        case NUM_OF_MEASUREMENT:
        default:
            break;
    }

    return NULL;
}

static int get_file_upload_mode()
{
    int rc = SR_ERR_OK;
    char xpath[XPATH_LEN];
    sr_val_t* value = NULL;
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* session = oam_netconf_ctx->netconf_ds_sess_running;
    char root_xpath[] = "/o-ran-performance-management:performance-measurement-objects";
    char* upload_mode[] = { "enable-file-upload", "enable-random-file-upload" };

    for (size_t i = 0; i < sizeof(upload_mode) / sizeof(upload_mode[0]); i++) {
        snprintf(xpath, sizeof(xpath), "%s/%s", root_xpath, upload_mode[i]);
        rc = sr_get_item(session, xpath, 0, &value);
        if (rc != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to get item:%s", upload_mode[i]);
            goto error;
        }

        if (value->data.bool_val) {
            sr_free_val(value);
            return i + 1;
        }
        sr_free_val(value);
    }

    return 0;

error:
    sr_free_val(value);
    return 0;
}

static uint16_t get_perf_mgmt_interval(const char* item)
{
    int rc = SR_ERR_OK;
    char xpath[XPATH_LEN];
    sr_val_t* values = NULL;
    size_t values_count = 0;
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* session = oam_netconf_ctx->netconf_ds_sess_running;
    uint16_t interval = 0;

    snprintf(xpath, sizeof(xpath), "/o-ran-performance-management:performance-measurement-objects/%s-interval", item);  // epe-measurement, transceiver-measurement, notification, file-upload
    rc = sr_get_items(session, xpath, 0, 0, &values, &values_count);
    if (rc != SR_ERR_OK) {
        OAM_LOG_INFO("get %s-interval of perf-mgmt failed", item);
        sr_free_values(values, values_count);
        return interval;
    }
    if (values_count == 0) {
        return 0;
    }

    interval = values[0].data.uint16_val;
    sr_free_values(values, values_count);

    return interval;
}

static inline int check_meas_obj_active(measurement_group_e meas_grp, uint32_t meas_obj_id)
{
    sr_val_t* val;
    const char* meas_grp_name = get_meas_grp_name(meas_grp);
    const char* meas_obj_name = get_meas_obj_name(meas_grp, meas_obj_id);

    if (meas_grp_name == NULL || meas_obj_name == NULL) {
        return -1;
    }

    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* session = oam_netconf_ctx->netconf_ds_sess_running;
    char path[XPATH_LEN] = { '\0' };
    char root_path[] = "/o-ran-performance-management:performance-measurement-objects";
    int active;

    snprintf(path, sizeof(path), "%s/%s-measurement-objects[measurement-object='%s']/active", root_path, meas_grp_name, meas_obj_name);
    if (sr_get_item(session, path, 0, &val)) {
        OAM_LOG_ERROR("there is no boolen value \"active\" of %s objects", meas_obj_name);
        sr_free_val(val);
        return -1;
    }
    active = val->data.bool_val;
    sr_free_val(val);

    return active;
}

static int get_transceiver_diagnostic_monitor_info(hal_config_t* hal_cfg, transceiver_diagnostic_monitor_info_t* info)
{
    size_t segment_size = 2;
    uint8_t result_data1[2] = { 0 };

    hal_cfg->retrieve_transceiver_diagnostic_monitor_info(result_data1, segment_size);
    OAM_LOG_DEBUG("transceiver diagnostic monitor type: 0x%02x%02x", result_data1[0], result_data1[1]);
    if (result_data1[0] & 0x40) { // diagnostic monitor type is enabled
        info->enabled = true;
        OAM_LOG_INFO("transceiver diagnostic monitor type is enabled");
    } else {
        OAM_LOG_INFO("transceiver diagnostic monitor type is disabled");
        return false;
    }
    if (result_data1[0] & 0x20) {  // internal calibrated
        info->internal_calibrated = true; 
        OAM_LOG_INFO("transceiver diagnostic monitor type is internal calibrated");
    }
    if (result_data1[0] & 0x10) {  // external calibrated
        info->external_calibrated = true;
    }
    if (info->internal_calibrated && info->external_calibrated) { // both internal and external calibrated
        info->enabled = false;
        OAM_LOG_ERROR("transceiver diagnostic monitor type is both internal and external calibrated, is is possible that the transceiver is not working properly");
        return false;
    }

    if (!info->enabled || !info->external_calibrated) { // if not enabled or not external calibrated, no need to get external calibration consts
        return true;
    }

    // get external calibration consts
    uint8_t result_data2[40] = { 0 };
    hal_cfg->retrieve_transceiver_external_calibrated_info(result_data2, 40);
    for (int i = 0; i <= 4; i++) {
        info->rx_power_calibration[4 - i] = (float)ntohl((uint32_t)result_data2[i * 4]);  // reverse order for rx power calibration
    }
    OAM_LOG_DEBUG("rx power calibration: %f, %f, %f, %f, %f", info->rx_power_calibration[0], info->rx_power_calibration[1], info->rx_power_calibration[2],
        info->rx_power_calibration[3], info->rx_power_calibration[4]);

    info->I_slope = ntohs((uint16_t)result_data2[20]);
    info->I_offset = (int16_t)ntohs((uint16_t)result_data2[22]);
    info->tx_power_slope = ntohs((uint16_t)result_data2[24]);
    info->tx_power_offset = (int16_t)ntohs((uint16_t)result_data2[26]);
    info->t_slope = ntohs((uint16_t)result_data2[28]);
    info->t_offset = (int16_t)ntohs((uint16_t)result_data2[30]);
    info->v_slope = ntohs((uint16_t)result_data2[32]);
    info->v_offset = (int16_t)ntohs((uint16_t)result_data2[34]);
    OAM_LOG_DEBUG("I_slope: %u, I_offset: %d, tx_power_slope: %u, tx_power_offset: %d, t_slope: %u, t_offset: %d, v_slope: %u, v_offset: %d", info->I_slope, info->I_offset,
        info->tx_power_slope, info->tx_power_offset, info->t_slope, info->t_offset, info->v_slope, info->v_offset);
    return true;
}

static bool get_transceiver_meas_info(hal_config_t* hal_cfg, perf_mgmt_transceiver_measurement_ctx_t* meas_ctx)
{
    return get_transceiver_diagnostic_monitor_info(hal_cfg, &meas_ctx->diag_monitor_info);  // 진단 모니터 정보 가져오기 from SFP 모듈 (hal_config_t)
}

static char* get_epe_measurment_object_unit_class(char* hardware_name)
{
    int rc = SR_ERR_OK;
    char xpath[XPATH_LEN];
    sr_val_t* values = NULL;
    size_t values_count = 0;
    char* hardware_class = NULL;
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* session = oam_netconf_ctx->netconf_ds_sess_running;

    snprintf(xpath, sizeof(xpath), "/ietf-hardware:hardware/component[name='%s']/class", hardware_name);
    rc = sr_get_items(session, xpath, 0, 0, &values, &values_count);
    if (rc != SR_ERR_OK) {
        OAM_LOG_INFO("get class of %s failed", hardware_name);
        sr_free_values(values, values_count);
        return NULL;
    }

    if (values_count == 0) {
        return NULL;
    }

    hardware_class = strdup(values[0].data.identityref_val);
    sr_free_values(values, values_count);

    return hardware_class;
}

static void print_epe_measurement_file_data(FILE* fp, epe_measurement_object_e epe_measurement_object, char* start_time, char* end_time, char* time_zone)
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();
    epe_measurement_result_t* epe_result = NULL;
    uint32_t epe_measurement_group_id = get_meas_grp_id(EPE_MEASUREMENT);
    char* hardware_class = NULL;
    char* epe_measurement_object_name = get_epe_measurement_obj_name(epe_measurement_object);
    char epe_measurement_report_format[] = ",%s,%+.4f";
    fprintf(fp, "%u,%s,%s%s,%s%s", epe_measurement_group_id, epe_measurement_object_name, start_time, time_zone, end_time, time_zone);
    for (size_t i = 0; i < perf_mgmt_ctx->epe_measurement_ctx.epe_result_list_num; i++) {
        epe_result = &perf_mgmt_ctx->epe_measurement_ctx.epe_result_list[i];
        if (((epe_measurement_object == TEMPERATURE) && (epe_result->sensor_type != SENSOR_TYPE_TEMPERATURE))) {
            continue;
        } else if (((epe_measurement_object != TEMPERATURE) && (epe_result->sensor_type != SENSOR_TYPE_POWER_RAIL))) {
            continue;
        }

        if ((hardware_class = get_epe_measurment_object_unit_class(epe_result->sensor_name)) == NULL) {
            continue;
        }

        fprintf(fp, ",%s,%s", hardware_class, epe_result->sensor_name);
        free(hardware_class);
        for (size_t j = 0; j < EPE_MEASUREMENT_REPORT_TYPE_NUM; j++) {
            if ((epe_result->active_map >> j) & 1) { // Check if the report type is active
                fprintf(fp, epe_measurement_report_format, (j == EPE_MEASUREMENT_REPORT_TYPE_AVERAGE) ? "avg" : get_epe_measurement_result_leaf_str(j),  
                    epe_result->monitor_result[epe_measurement_object][j]); // Print the report type and its value
            }
        }
    }
    fprintf(fp, "\n");
}

static void print_transceiver_measurement_file_data(FILE* fp, transceiver_measurement_object_e meas_obj_id, char* start_time, char* end_time, char* time_zone)
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();
    transceiver_result_info_t* result = &perf_mgmt_ctx->transceiver_meas_ctx.result[meas_obj_id];
    uint32_t group_id = get_meas_grp_id(TRANSCEIVER_MEASUREMENT);
    char* meas_obj_name = get_transceiver_meas_obj_name(meas_obj_id);
    fprintf(fp, "%u,%s,%s%s,%s%s", group_id, meas_obj_name, start_time, time_zone, end_time, time_zone);
    if (result->min_active) {
        fprintf(fp, ",min,%+.4f", result->min);
    }
    if (result->max_active) {
        fprintf(fp, ",max,%+.4f", result->max);
    }
    if (result->first_active) {
        fprintf(fp, ",first,%+.4f", result->first);
    }
    if (result->latest_active) {
        fprintf(fp, ",latest,%+.4f", result->latest);
    }
    if (result->bin_count > 0) {
        fprintf(fp, ",freq-bin-table[%+.4f,%+.4f]", result->lower_bound, result->upper_bound);
        for (uint16_t i = 0; i < result->bin_count; i++) {
            fprintf(fp, ",index,%u,value,%u", i, result->freq_bin_table[i]);
        }
    }
    fprintf(fp, "\n");
    return;
}

static void perf_mgmt_print_file(sr_session_ctx_t* running_session, FILE* fp, perf_mgmt_measurement_result_t* measurement_result)
{
    int ret = SR_ERR_OK;
    char xpath[XPATH_LEN];
    uint64_t meas_count = 0;
    uint32_t meas_obj_id = 0;
    sr_val_t* values = NULL;
    size_t values_count = 0;
    char root_xpath[] = "/o-ran-performance-management:performance-measurement-objects";
    char time_zone[TIME_ZONE_STR_LEN];
    char start_time[LOCAL_TIME_STR_LEN];
    char end_time[LOCAL_TIME_STR_LEN];
    time_t now = time(NULL);

    ret = get_local_timezone(FILE_CONTENT_TIME_ZONE_FORMAT, time_zone);
    if (ret != 0) {
        OAM_LOG_ERROR("failed to get time zone");
        return;
    }

    time_t start = now - measurement_result->measurement_interval;
    strftime(start_time, sizeof(start_time), FILE_NAME_LOCAL_TIME_FORMAT, localtime(&start));
    strftime(end_time, sizeof(end_time), FILE_NAME_LOCAL_TIME_FORMAT, localtime(&now));

    uint16_t meas_grp_id = measurement_result->measurement_group;
    const char* meas_grp_name = get_meas_grp_name(meas_grp_id);  // Get the measurement group name. 1~4

    snprintf(xpath, sizeof(xpath), "%s/%s-measurement-objects/active", root_xpath, meas_grp_name);
    ret = sr_get_items(running_session, xpath, 0, 0, &values, &values_count);
    if (ret != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of %s-measurement-objects \"active\"", meas_grp_name);
        return;
    }

    for (size_t i = 0; i < values_count; i++) {
        if (!values[i].data.bool_val) {
            continue;
        }
        meas_obj_id = (uint32_t)get_meas_obj_id(strchr(values[i].xpath, '\'') + 1, meas_grp_id);  // last key name
        if (meas_grp_id == TX_MEASUREMENT) {
            oam_tx_counters_t* tx_counters_result = (oam_tx_counters_t*)measurement_result->counter_result->measurements;
            meas_count = get_tx_meas_result(meas_obj_id, tx_counters_result);
            ret = fprintf(fp, TX_MEASUREMNT_FORMAT_STR, get_meas_grp_id(meas_grp_id), get_tx_meas_obj_name(meas_obj_id), start_time, time_zone, end_time, time_zone,
                get_oam_perf_mgmt_ctx()->radio_unit_name, meas_count);
            if (ret < 0) {
                OAM_LOG_ERROR("failed to print measurement result into file, %s", strerror(errno));
                return;
            }
        } else if (meas_grp_id == RX_WINDOW_MEASUREMENT) {
            oam_rx_counters_t* rx_win_counters_result = (oam_rx_counters_t*)measurement_result->counter_result->measurements;
            meas_count = get_rx_win_meas_result(meas_obj_id, rx_win_counters_result);
            ret = fprintf(fp, RX_WINDOW_MEASUREMNT_FORMAT_STR, get_meas_grp_id(meas_grp_id), get_rx_win_meas_obj_name(meas_obj_id), start_time, time_zone, end_time, time_zone,
                get_oam_perf_mgmt_ctx()->radio_unit_name, meas_count);
            if (ret < 0) {
                OAM_LOG_ERROR("failed to print measurement result into file, %s", strerror(errno));
                return;
            }
        } else if (meas_grp_id == EPE_MEASUREMENT) {
            print_epe_measurement_file_data(fp, meas_obj_id, start_time, end_time, time_zone);
        } else if (meas_grp_id == TRANSCEIVER_MEASUREMENT) {
            print_transceiver_measurement_file_data(fp, meas_obj_id, start_time, end_time, time_zone);
        } else {
            OAM_LOG_ERROR("unkown measurement result");
            return;
        }
    }
    sr_free_values(values, values_count);
    return;
}

static int perf_mgmt_write_upload_file(sr_session_ctx_t* running_session, perf_mgmt_measurement_result_t* measurement_result)  // Write the measurement result to the upload file.
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();

    if ((perf_mgmt_ctx->file_upload_ctx.file_upload_mode == 0) || (perf_mgmt_ctx->file_upload_ctx.file_upload_interval == 0)) {
        return 0;
    }

    if (strlen(perf_mgmt_ctx->curr_csv_file_path) == 0) {
        return -1;
    }

    pthread_mutex_lock(&perf_mgmt_ctx->file_mutex);
    perf_mgmt_print_file(running_session, perf_mgmt_ctx->csv_fp, measurement_result);  // Print the measurement result to the file.
    fflush(perf_mgmt_ctx->csv_fp);
    pthread_mutex_unlock(&perf_mgmt_ctx->file_mutex);

    return 0;
}

static int fill_rx_win_meas_result_unit_RU(sr_session_ctx_t* running_session, sr_session_ctx_t* operational_session, measurement_counter_result_ind_t* result)
{
    int rc = SR_ERR_OK;
    char xpath[XPATH_LEN];
    char rx_win_meas_count_str[RESULT_STR_LEN];
    uint64_t rx_win_meas_count = 0;
    uint32_t rx_win_meas_obj_id = 0;
    char* support_rx_win_meas_obj_name = NULL;
    sr_val_t* values = NULL;
    size_t values_count = 0;
    oam_rx_counters_t* rx_win_counters_result = (oam_rx_counters_t*)result->measurements;
    char root_xpath[] = "/o-ran-performance-management:performance-measurement-objects";

    snprintf(xpath, sizeof(xpath), "%s/rx-window-measurement-objects/active", root_xpath);

    rc = sr_get_items(running_session, xpath, 0, 0, &values, &values_count);
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of rx-window-measurement-objects \"active\"");
        goto error;
    }

    for (size_t i = 0; i < values_count; i++) {
        rx_win_meas_obj_id = (uint32_t)get_meas_obj_id(strchr(values[i].xpath, '\'') + 1, RX_WINDOW_MEASUREMENT);
        support_rx_win_meas_obj_name = get_rx_win_meas_obj_name(rx_win_meas_obj_id);
        snprintf(xpath, sizeof(xpath), "%s/rx-window-measurement-objects[measurement-object='%s']/name", root_xpath, support_rx_win_meas_obj_name);
        rc = sr_set_item_str(operational_session, xpath, get_oam_perf_mgmt_ctx()->radio_unit_name, NULL, 0);
        if (rc != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to set the value of rx-window-measurement-objects \"name\"");
            goto error;
        }

        snprintf(xpath, sizeof(xpath), "%s/rx-window-measurement-objects[measurement-object='%s']/count", root_xpath, support_rx_win_meas_obj_name);
        if (!values[i].data.bool_val) {
            rx_win_meas_count = 0;
        } else {
            rx_win_meas_count = get_rx_win_meas_result(rx_win_meas_obj_id, rx_win_counters_result);
        }
        snprintf(rx_win_meas_count_str, sizeof(rx_win_meas_count_str), "%" PRIu64, rx_win_meas_count);
        rc = sr_set_item_str(operational_session, xpath, rx_win_meas_count_str, NULL, 0);
        if (rc != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to set the value of rx-window-measurement-objects \"count\"");
            goto error;
        }

        sr_apply_changes(operational_session, 0);
    }

error:
    sr_free_values(values, values_count);
    return rc;
}

static int fill_tx_meas_result_unit_RU(sr_session_ctx_t* running_session, sr_session_ctx_t* operational_session, measurement_counter_result_ind_t* result)
{
    int rc = SR_ERR_OK;

    char xpath[XPATH_LEN];
    char tx_meas_count_str[RESULT_STR_LEN];
    uint64_t tx_meas_count = 0;
    uint32_t tx_meas_obj_id = 0;
    char* support_tx_meas_obj_name = NULL;
    sr_val_t* values = NULL;
    size_t values_count = 0;
    oam_tx_counters_t* tx_counters_result = (oam_tx_counters_t*)result->measurements;
    char root_xpath[] = "/o-ran-performance-management:performance-measurement-objects";

    snprintf(xpath, sizeof(xpath), "%s/tx-measurement-objects/active", root_xpath);

    rc = sr_get_items(running_session, xpath, 0, 0, &values, &values_count);
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of tx-measurement-objects \"active\"");
        goto error;
    }

    for (size_t i = 0; i < values_count; i++) {
        tx_meas_obj_id = (uint32_t)get_meas_obj_id(strchr(values[i].xpath, '\'') + 1, TX_MEASUREMENT);
        support_tx_meas_obj_name = get_tx_meas_obj_name(tx_meas_obj_id);
        snprintf(xpath, sizeof(xpath), "%s/tx-measurement-objects[measurement-object='%s']/name", root_xpath, support_tx_meas_obj_name);
        rc = sr_set_item_str(operational_session, xpath, get_oam_perf_mgmt_ctx()->radio_unit_name, NULL, 0);
        if (rc != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to set the value of tx-measurement-objects \"name\"");
            goto error;
        }

        snprintf(xpath, sizeof(xpath), "%s/tx-measurement-objects[measurement-object='%s']/count", root_xpath, support_tx_meas_obj_name);
        if (!values[i].data.bool_val) {
            tx_meas_count = 0;
        } else {
            tx_meas_count = get_tx_meas_result(tx_meas_obj_id, tx_counters_result);
        }
        snprintf(tx_meas_count_str, sizeof(tx_meas_count_str), "%" PRIu64, tx_meas_count);
        rc = sr_set_item_str(operational_session, xpath, tx_meas_count_str, NULL, 0);
        if (rc != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to set the value of tx-measurement-objects \"count\" to %s, error: %s", tx_meas_count_str, sr_strerror(rc));
            goto error;
        }

        sr_apply_changes(operational_session, 0);
    }

error:
    sr_free_values(values, values_count);
    return rc;
}

inline const char* epe_measurement_report_enum_to_string(epe_measurement_report_type_e epe_measurement_report)
{
    switch (epe_measurement_report) {
        case EPE_MEASUREMENT_REPORT_TYPE_MAXIMUM:
            return "MAXIMUM";
        case EPE_MEASUREMENT_REPORT_TYPE_MINIMUM:
            return "MINIMUM";
        case EPE_MEASUREMENT_REPORT_TYPE_AVERAGE:
            return "AVERAGE";
        case EPE_MEASUREMENT_REPORT_TYPE_NUM:
        default:
            return NULL;
    }
}

static inline int epe_measurement_report_string_to_enum(const char* report_enum)
{
    if (strcmp(report_enum, "MAXIMUM") == 0) {
        return EPE_MEASUREMENT_REPORT_TYPE_MAXIMUM;
    } else if (strcmp(report_enum, "MINIMUM") == 0) {
        return EPE_MEASUREMENT_REPORT_TYPE_MINIMUM;
    } else if (strcmp(report_enum, "AVERAGE") == 0) {
        return EPE_MEASUREMENT_REPORT_TYPE_AVERAGE;
    } else {
        return -1;
    }
}

static char* get_epe_measurement_result_leaf_str(uint32_t epe_measurement_report)
{
    switch (epe_measurement_report) {
        case EPE_MEASUREMENT_REPORT_TYPE_MAXIMUM:
            return "max";
        case EPE_MEASUREMENT_REPORT_TYPE_MINIMUM:
            return "min";
        case EPE_MEASUREMENT_REPORT_TYPE_AVERAGE:
            return "average";
        case EPE_MEASUREMENT_REPORT_TYPE_NUM:
        default:
            return NULL;
    }
}

// report_info 
int set_all_epe_measurement_result_value(sr_session_ctx_t* operational_session, const char* xpath_format, epe_measurement_object_e epe_measurement_object,
    epe_measurement_report_type_e epe_measurement_report)
{
    int rc = SR_ERR_OK;
    char xpath[XPATH_LEN];
    char sub_xpath[XPATH_LEN / 2];
    char result_str[RESULT_STR_LEN];
    epe_measurement_result_t* epe_result = NULL;
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();

    char* epe_measurement_object_name = get_epe_measurement_obj_name(epe_measurement_object); // index에 따른 object name ; TEMPERATURE, POWER, VOLTAGE, CURRENT
    char* epe_measurement_result_leaf_str = get_epe_measurement_result_leaf_str(epe_measurement_report);  // min, max, average

    for (size_t i = 0; i < perf_mgmt_ctx->epe_measurement_ctx.epe_result_list_num; i++) {  
        epe_result = &perf_mgmt_ctx->epe_measurement_ctx.epe_result_list[i];
        if (((epe_measurement_object == TEMPERATURE) && (epe_result->sensor_type != SENSOR_TYPE_TEMPERATURE))) {
            continue;
        } else if (((epe_measurement_object != TEMPERATURE) && (epe_result->sensor_type != SENSOR_TYPE_POWER_RAIL))) {
            continue;
        }

        epe_result->active_map = epe_result->active_map | (1 << epe_measurement_report);  // set active map for epe measurement report
        snprintf(sub_xpath, sizeof(sub_xpath), "epe-measurement-resultv2[object-unit-id='%s']/%s", epe_result->sensor_name, epe_measurement_result_leaf_str);
        snprintf(xpath, sizeof(xpath), xpath_format, epe_measurement_object_name, sub_xpath);
        snprintf(result_str, sizeof(result_str), "%+.4f", epe_result->monitor_result[epe_measurement_object][epe_measurement_report]);
        rc = sr_set_item_str(operational_session, xpath, result_str, NULL, 0); // set epe-measurement-resultv2 leaf value
            OAM_LOG_ERROR("failed to set the value of epe-measurement-objects \"%s\", for %s", epe_measurement_result_leaf_str, epe_result->sensor_name);
            return rc;
        }
    }

    sr_apply_changes(operational_session, 0);
    return rc;
}

int poll_epe_measurement_report_info_set_result(sr_session_ctx_t* sess_for_run, sr_session_ctx_t* operational_session, epe_measurement_object_e epe_measurement_object) // Poll EPE measurement report info and set result
{
    int rc = SR_ERR_OK;
    char xpath[XPATH_LEN];
    sr_val_t* values = NULL;
    size_t values_count = 0;
    char xpath_format[] = "/o-ran-performance-management:performance-measurement-objects/epe-measurement-objects[measurement-object='%s']/%s";
    char* epe_measurement_object_name = get_epe_measurement_obj_name(epe_measurement_object);  //"TEMPERATURE", "POWER", "VOLTAGE", "CURRENT",

    snprintf(xpath, sizeof(xpath), xpath_format, epe_measurement_object_name, "report-info");  // leaflist 
    rc = sr_get_items(sess_for_run, xpath, 0, 0, &values, &values_count); // get report-info leaflist on running datastore
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of epe-measurement-objects \"active\"");
        sr_free_values(values, values_count);
        return rc;
    }

    for (size_t i = 0; i < values_count; i++) {
        epe_measurement_report_type_e epe_measurement_report = epe_measurement_report_string_to_enum(values[i].data.enum_val);  //    EPE_MEASUREMENT_REPORT_TYPE_MAXIMUM = 0x00, EPE_MEASUREMENT_REPORT_TYPE_MINIMUM, EPE_MEASUREMENT_REPORT_TYPE_AVERAGE, EPE_MEASUREMENT_REPORT_TYPE_NUM
        rc = set_all_epe_measurement_result_value(operational_session, xpath_format, epe_measurement_object, epe_measurement_report);  // set all epe measurement result value
        if (rc != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to set all epe measurement result value");
            sr_free_values(values, values_count);
            return rc;
        }
    }

    sr_free_values(values, values_count);
    return rc;
}

static int fill_epe_measurement_result(sr_session_ctx_t* running_session, sr_session_ctx_t* operational_session)
{
    int rc = SR_ERR_OK;
    uint32_t epe_meas_obj_id = 0;
    sr_val_t* values = NULL;
    size_t values_count = 0;

    rc = sr_get_items(running_session, "/o-ran-performance-management:performance-measurement-objects/epe-measurement-objects/active", 0, 0, &values, &values_count);
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of epe-measurement-objects \"active\"");
        goto error;
    }

    for (size_t i = 0; i < values_count; i++) {  
        if (!values[i].data.bool_val) {
            continue;
        }

        epe_meas_obj_id = (uint32_t)get_meas_obj_id(strchr(values[i].xpath, '\'') + 1, EPE_MEASUREMENT);  // get meas obj name list ; epe-measurement-objects[measurement-object='TEMPERATURE']
        rc = poll_epe_measurement_report_info_set_result(running_session, operational_session, epe_meas_obj_id);  // active 인 epe-measurement-objects 에 대해서 report-info 를 poll 해서 result 에 저장
        if (rc != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to poll epe-measurement report info");
            goto error;
        }
    }
    sr_free_values(values, values_count);
    return rc;

error:
    sr_free_values(values, values_count);
    return rc;
}

/**
 * @brief Write the transceiver measurement result into sysrepo. And record the active flag of result_info for later use.
 */
static void write_transceiver_meas_result(sr_session_ctx_t* running_session, sr_session_ctx_t* operational_session, perf_mgmt_transceiver_measurement_ctx_t* ctx,
    uint32_t meas_obj_id)
{
    sr_session_ctx_t* sess_for_oper = operational_session;
    sr_session_ctx_t* sess_for_run = running_session;

    sr_val_t* values = NULL;
    size_t values_count = 0;
    char time_xpath[XPATH_LEN];
    char value_xpath[XPATH_LEN];

    char root_xpath[XPATH_LEN / 2];
    char* meas_obj_name = get_transceiver_meas_obj_name(meas_obj_id);
    snprintf(root_xpath, sizeof(root_xpath),
        "/o-ran-performance-management:performance-measurement-objects/transceiver-measurement-objects[measurement-object='%s']/transceiver-measurement-result[object-unit-id='0']",
        meas_obj_name);

    snprintf(value_xpath, sizeof(value_xpath), "/o-ran-performance-management:performance-measurement-objects/transceiver-measurement-objects[measurement-object='%s']/report-info",
        meas_obj_name);
    int ret = sr_get_items(sess_for_run, value_xpath, 0, 0, &values, &values_count);
    if (ret != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of transceiver-measurement-objects \"active\"");
        goto error;
    }
    bool is_write = false;
    char* time_str = NULL;
    sr_val_t value = { 0 };
    value.type = SR_DECIMAL64_T;
    for (size_t i = 0; i < values_count; i++) {
        if (strcmp(values[i].data.enum_val, "MINIMUM") == 0) {
            is_write = true;
            ctx->result[meas_obj_id].min_active = true;
            snprintf(time_xpath, sizeof(time_xpath), "%s/min/time", root_xpath);
            snprintf(value_xpath, sizeof(value_xpath), "%s/min/value", root_xpath);
            value.data.decimal64_val = ctx->result[meas_obj_id].min;
            time_str = ctx->result[meas_obj_id].min_time;
        } else if (strcmp(values[i].data.enum_val, "MAXIMUM") == 0) {
            is_write = true;
            ctx->result[meas_obj_id].max_active = true;
            snprintf(time_xpath, sizeof(time_xpath), "%s/max/time", root_xpath);
            snprintf(value_xpath, sizeof(value_xpath), "%s/max/value", root_xpath);
            value.data.decimal64_val = ctx->result[meas_obj_id].max;
            time_str = ctx->result[meas_obj_id].max_time;
        } else if (strcmp(values[i].data.enum_val, "FIRST") == 0) {
            is_write = true;
            ctx->result[meas_obj_id].first_active = true;
            snprintf(time_xpath, sizeof(time_xpath), "%s/first/time", root_xpath);
            snprintf(value_xpath, sizeof(value_xpath), "%s/first/value", root_xpath);
            value.data.decimal64_val = ctx->result[meas_obj_id].first;
            time_str = ctx->result[meas_obj_id].first_time;
        } else if (strcmp(values[i].data.enum_val, "LATEST") == 0) {
            is_write = true;
            ctx->result[meas_obj_id].latest_active = true;
            snprintf(time_xpath, sizeof(time_xpath), "%s/latest/time", root_xpath);
            snprintf(value_xpath, sizeof(value_xpath), "%s/latest/value", root_xpath);
            value.data.decimal64_val = ctx->result[meas_obj_id].latest;
            time_str = ctx->result[meas_obj_id].latest_time;
        } else {
            OAM_LOG_WARN("unsupported transceiver measurement report type %s", values[i].data.enum_val);
            continue;
        }
        if (is_write) {
            if (time_str != NULL && strlen(time_str) > 0) {
                ret = sr_set_item_str(sess_for_oper, time_xpath, time_str, NULL, 0);
                if (ret != SR_ERR_OK) {
                    OAM_LOG_ERROR("failed to set transceiver-measurement-objects time");
                    goto error;
                }
            }
            ret = sr_set_item(sess_for_oper, value_xpath, &value, 0);
            if (ret != SR_ERR_OK) {
                OAM_LOG_ERROR("failed to set transceiver-measurement-objects value");
                goto error;
            }
            is_write = false;
        }
    }
    value.type = SR_UINT32_T;
    for (uint16_t i = 0; i < ctx->result[meas_obj_id].bin_count; i++) {
        snprintf(value_xpath, sizeof(value_xpath), "%s/frequency-bin-table[bin-id='%u']/value", root_xpath, i);
        value.data.uint32_val = ctx->result[meas_obj_id].freq_bin_table[i];
        ret = sr_set_item(sess_for_oper, value_xpath, &value, 0);
        if (ret != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to set transceiver-measurement-objects bin value");
            goto error;
        }
    }

error:
    sr_apply_changes(sess_for_oper, 0);
    sr_free_values(values, values_count);
    return;
}

/**
 * @brief Extract the transceiver measurement object name from XPATH.
 * For example, the xpath is "/o-ran-performance-management:performance-measurement-objects/transceiver-measurement-objects[measurement-object='RX_POWER']/active",
 * the function will return "RX_POWER".
 */
static const char* extract_transceiver_meas_obj_name(const char* xpath)
{
    static char meas_obj_id[32];
    const char* start = strstr(xpath, "measurement-object='");
    if (start) {
        start += strlen("measurement-object='");
        const char* end = strchr(start, '\'');
        if (end) {
            size_t len = end - start;
            strncpy(meas_obj_id, start, len);
            meas_obj_id[len] = '\0';
            return meas_obj_id;
        }
    }
    return NULL;
}

/**
 * @brief Calculate the transceiver measurement result from record data, and write them into sysrepo according to "active" value.
 */
static int fill_transceiver_meas_result(sr_session_ctx_t* running_session, sr_session_ctx_t* operational_session, perf_mgmt_measurement_result_t* measurement_result)
{
    perf_mgmt_transceiver_measurement_ctx_t* transceiver_meas_ctx = &get_oam_perf_mgmt_ctx()->transceiver_meas_ctx;
    const char* meas_obj_name = NULL;
    uint32_t meas_obj_id = 0;
    sr_val_t* values = NULL;
    size_t values_count = 0;

    char xpath[XPATH_LEN];
    snprintf(xpath, sizeof(xpath), "/o-ran-performance-management:performance-measurement-objects/transceiver-measurement-objects/active");

    int ret = sr_get_items(running_session, xpath, 0, 0, &values, &values_count);
    if (ret != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of transceiver-measurement-objects \"active\"");
        goto error;
    }

    for (size_t i = 0; i < values_count; i++) {
        meas_obj_name = extract_transceiver_meas_obj_name(values[i].xpath);
        meas_obj_id = (uint32_t)get_meas_obj_id(meas_obj_name, TRANSCEIVER_MEASUREMENT);

        if (values[i].data.bool_val) {
            transceiver_meas_ctx->result[meas_obj_id].active = true;
            get_transceiver_meas_result(transceiver_meas_ctx, meas_obj_id);
        } else {
            transceiver_meas_ctx->result[meas_obj_id].active = false;
        }

        write_transceiver_meas_result(running_session, operational_session, transceiver_meas_ctx, meas_obj_id);
    }
    measurement_result->transceiver_result = transceiver_meas_ctx->result;

error:
    sr_free_values(values, values_count);
    return ret;
}

static int perf_mgmt_fill_meas_result(sr_session_ctx_t* running_session, sr_session_ctx_t* operational_session, perf_mgmt_measurement_result_t* measurement_result) 
{
    int rc = SR_ERR_OK;

    switch (measurement_result->measurement_group) {
        case TX_MEASUREMENT: {
            rc = fill_tx_meas_result_unit_RU(running_session, operational_session, measurement_result->counter_result);
            break;
        }

        case RX_WINDOW_MEASUREMENT: {
            rc = fill_rx_win_meas_result_unit_RU(running_session, operational_session, measurement_result->counter_result);
            break;
        }

        case EPE_MEASUREMENT: {
            rc = fill_epe_measurement_result(running_session, operational_session);  // Poll EPE measurement report info and set result
            break;
        }

        case TRANSCEIVER_MEASUREMENT: {
            rc = fill_transceiver_meas_result(running_session, operational_session, measurement_result);
            break;
        }

        case SYMBOL_RSSI_MEASUREMENT:
        case NUM_OF_MEASUREMENT:
        default:
            break;
    }

    return rc;
}

static void write_latest_notif_data(measurement_group_e meas_grp, uint32_t meas_obj_id, sr_val_t** notif_val, uint8_t item_cnt)
{
    int ret = 0;
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();

    ret = check_meas_obj_active(meas_grp, meas_obj_id);  // Check if the measurement object is active.
    if (ret < 0) {
        return;
    }

    perf_mgmt_latest_notif_t* latest = &perf_mgmt_ctx->notif_ctx.latest_notif[meas_grp][meas_obj_id];
    if (ret == 0) {
        latest->active = ret;
        return;
    }

    if (latest->data == NULL) {
        latest->data = calloc(item_cnt, sizeof(sr_val_t));
    } else if (latest->data_cnt < item_cnt) {
        free(latest->data);
        latest->data = calloc(item_cnt, sizeof(sr_val_t));
    }

    uint8_t i = 0;
    for (; i < item_cnt; i++) {
        memcpy(&latest->data[i], notif_val[i], sizeof(sr_val_t));
    }
    latest->active = ret;
    latest->data_cnt = item_cnt;

    return;
}

static void write_multi_notif_data(oam_perf_mgmt_ctx_t* perf_mgmt_ctx, sr_val_t** notif_val, uint8_t item_cnt)  // Write multiple notification data to the linked list.
{
    for (size_t i = 0; i < item_cnt; i++) {

        if (!perf_mgmt_ctx->notif_ctx.measurement_stats.data_head) {
            perf_mgmt_ctx->notif_ctx.measurement_stats.data_head = calloc(1, sizeof(perf_mgmt_notif_data_t));
            if (perf_mgmt_ctx->notif_ctx.measurement_stats.data_head == NULL) {
                OAM_LOG_ERROR("can't alloc mem for perf_mgmt_ctx link node");
                return;
            }
            perf_mgmt_ctx->notif_ctx.measurement_stats.data_head->values = notif_val[i];
            perf_mgmt_ctx->notif_ctx.measurement_stats.data_cnt += 1;
            perf_mgmt_ctx->notif_ctx.measurement_stats.data_end = perf_mgmt_ctx->notif_ctx.measurement_stats.data_head;

        } else {
            perf_mgmt_ctx->notif_ctx.measurement_stats.data_end->next = calloc(1, sizeof(perf_mgmt_notif_data_t));
            if (perf_mgmt_ctx->notif_ctx.measurement_stats.data_end->next == NULL) {
                OAM_LOG_ERROR("can't alloc mem for perf_mgmt_ctx link node");
                return;
            }
            perf_mgmt_ctx->notif_ctx.measurement_stats.data_end->next->values = notif_val[i];
            perf_mgmt_ctx->notif_ctx.measurement_stats.data_cnt += 1;
            perf_mgmt_ctx->notif_ctx.measurement_stats.data_end = perf_mgmt_ctx->notif_ctx.measurement_stats.data_end->next;
        }
    }
}

/* Check if notification is enabled. */
static int check_notif_enabled()
{
    int ret;
    sr_val_t* value = NULL;
    char xpath[128];
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* session = oam_netconf_ctx->netconf_ds_sess_running;
    snprintf(xpath, sizeof(xpath), "/o-ran-performance-management:performance-measurement-objects/notification-interval");
    ret = sr_get_item(session, xpath, 0, &value);
    if (ret == SR_ERR_NOT_FOUND) {
        OAM_LOG_INFO("notification of performance-mgmt is not enabled");
        return -1;
    }
    if (value == NULL) {
        OAM_LOG_ERROR("can't get performance measurement notification interval");
        return -1;
    }
    if (value->data.uint16_val == 0) {
        OAM_LOG_INFO("notification interval of performance-mgmt is 0, do not send");
        sr_free_val(value);
        return -1;
    }

    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();
    if (!perf_mgmt_ctx->notif_ctx.tid) {
        OAM_LOG_INFO("notification sending thread of performance-mgmt is not running");
        return -1;
    }
    return 0;
}

static void write_tx_rx_notif(measurement_group_e meas_grp, uint32_t meas_obj_id, const uint64_t count, const uint16_t interval)
{
    sr_val_t* notif_val[TX_RX_WIN_NOTIF_ITEM_CNT];
    char xpath[XPATH_LEN];
    char start_time_str[LOCAL_TIME_STR_LEN];
    char end_time_str[LOCAL_TIME_STR_LEN];
    char* time_str = NULL;
    time_t now;
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();
    char notif_xpath[] = "/o-ran-performance-management:measurement-result-stats";
    uint32_t index = ++(perf_mgmt_ctx->notif_ctx.latest_notif[meas_grp][meas_obj_id].index);
    const char* meas_grp_name = get_meas_grp_name(meas_grp);
    const char* meas_obj_name = get_meas_obj_name(meas_grp, meas_obj_id);

    /* notif_val is freed one by one, so it need to be allocated one by one. */
    uint8_t i = 0;
    uint8_t j = 0;
    for (; i < TX_RX_WIN_NOTIF_ITEM_CNT; i++) {
        notif_val[i] = (sr_val_t*)calloc(1, sizeof(sr_val_t));
        if (notif_val[i] == NULL) {
            OAM_LOG_ERROR("can't alloc mem for notif_val");
            for (; j < i; j++) {
                free(notif_val[j]);
            }
            return;
        }
    }
    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/measurement-object", notif_xpath, meas_grp_name, meas_obj_name);
    notif_val[0]->xpath = strdup(xpath);
    notif_val[0]->type = SR_ENUM_T;
    notif_val[0]->data.enum_val = strdup(meas_obj_name);

    now = time(NULL);
    ly_time_time2str(now - interval, NULL, &time_str);
    strncpy(start_time_str, time_str, sizeof(start_time_str));
    free(time_str);
    notif_val[1]->type = SR_STRING_T;
    notif_val[1]->data.string_val = strdup(start_time_str);

    ly_time_time2str(now, NULL, &time_str);
    strncpy(end_time_str, time_str, sizeof(end_time_str));
    free(time_str);
    notif_val[2]->type = SR_STRING_T;
    notif_val[2]->data.string_val = strdup(end_time_str);

    notif_val[3]->type = SR_STRING_T;
    notif_val[3]->data.string_val = strdup(perf_mgmt_ctx->radio_unit_name);

    notif_val[4]->type = SR_UINT64_T;
    notif_val[4]->data.uint64_val = count;

    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/start-time", notif_xpath, meas_grp_name, meas_obj_name);
    notif_val[1]->xpath = strdup(xpath);
    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/end-time", notif_xpath, meas_grp_name, meas_obj_name);
    notif_val[2]->xpath = strdup(xpath);
    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/name", notif_xpath, meas_grp_name, meas_obj_name);
    notif_val[3]->xpath = strdup(xpath);
    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/count", notif_xpath, meas_grp_name, meas_obj_name);
    notif_val[4]->xpath = strdup(xpath);
    write_latest_notif_data(meas_grp, meas_obj_id, notif_val, TX_RX_WIN_NOTIF_ITEM_CNT);

    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/start-time", notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name,
        index);
    notif_val[1]->xpath = strdup(xpath);
    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/end-time", notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name,
        index);
    notif_val[2]->xpath = strdup(xpath);
    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/name", notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name, index);
    notif_val[3]->xpath = strdup(xpath);
    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/count", notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name,
        index);
    notif_val[4]->xpath = strdup(xpath);
    write_multi_notif_data(perf_mgmt_ctx, notif_val, TX_RX_WIN_NOTIF_ITEM_CNT);
}

static int write_rx_win_meas_notification_unit_RU(sr_session_ctx_t* running_session, measurement_counter_result_ind_t* result)
{
    int rc = SR_ERR_OK;
    char xpath[XPATH_LEN];
    uint64_t rx_win_meas_count = 0;
    uint32_t rx_win_meas_obj_id = 0;
    sr_val_t* values = NULL;
    size_t values_count = 0;
    oam_rx_counters_t* rx_win_counters_result = (oam_rx_counters_t*)result->measurements;
    char root_xpath[] = "/o-ran-performance-management:performance-measurement-objects";

    snprintf(xpath, sizeof(xpath), "%s/rx-window-measurement-objects/active", root_xpath);
    rc = sr_get_items(running_session, xpath, 0, 0, &values, &values_count);
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of rx-window-measurement-objects \"active\"");
        goto error;
    }

    for (size_t i = 0; i < values_count; i++) {
        if (!values[i].data.bool_val) {
            continue;
        }
        rx_win_meas_obj_id = (uint32_t)get_meas_obj_id(strchr(values[i].xpath, '\'') + 1, RX_WINDOW_MEASUREMENT);
        rx_win_meas_count = get_rx_win_meas_result(rx_win_meas_obj_id, rx_win_counters_result);

        write_tx_rx_notif(RX_WINDOW_MEASUREMENT, rx_win_meas_obj_id, rx_win_meas_count, result->measurement_interval);
    }
    sr_free_values(values, values_count);
    return rc;

error:
    sr_free_values(values, values_count);
    return rc;
}

static int write_tx_meas_notification_unit_RU(sr_session_ctx_t* running_session, measurement_counter_result_ind_t* result)
{
    int rc = SR_ERR_OK;
    char xpath[XPATH_LEN];
    uint64_t tx_meas_count = 0;
    uint32_t tx_meas_obj_id = 0;
    sr_val_t* values = NULL;
    size_t values_count = 0;
    oam_tx_counters_t* tx_counters_result = (oam_tx_counters_t*)result->measurements;
    char root_xpath[] = "/o-ran-performance-management:performance-measurement-objects";

    snprintf(xpath, sizeof(xpath), "%s/tx-measurement-objects/active", root_xpath);
    rc = sr_get_items(running_session, xpath, 0, 0, &values, &values_count);
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of tx-measurement-objects \"active\"");
        goto error;
    }

    for (size_t i = 0; i < values_count; i++) {
        if (!values[i].data.bool_val) {
            continue;
        }

        tx_meas_obj_id = (uint32_t)get_meas_obj_id(strchr(values[i].xpath, '\'') + 1, TX_MEASUREMENT);
        tx_meas_count = get_tx_meas_result(tx_meas_obj_id, tx_counters_result);
        write_tx_rx_notif(TX_MEASUREMENT, tx_meas_obj_id, tx_meas_count, result->measurement_interval);
    }
    sr_free_values(values, values_count);
    return rc;

error:
    sr_free_values(values, values_count);
    return rc;
}

// Allocate memory for notification data based on measurement group and object ID
static int alloc_memory_for_notification_data(oam_perf_mgmt_ctx_t* perf_mgmt_ctx, sr_val_t*** notif_val, measurement_group_e meas_grp, uint32_t meas_obj_id, uint8_t active_map)
{
    uint32_t notify_item_cnt = 0;
    uint32_t sensor_cnt = 0;
    uint32_t active_item_cnt = 0;
    sr_val_t** notif_val_list = NULL;
    if (meas_grp == EPE_MEASUREMENT) {
        if (meas_obj_id == TEMPERATURE) {
            sensor_cnt += perf_mgmt_ctx->epe_measurement_ctx.temperature_sensor_num;
        } else if (meas_obj_id != TEMPERATURE) {
            sensor_cnt += perf_mgmt_ctx->epe_measurement_ctx.power_rail_sensor_num;
        }

        for (size_t i = 0; i < EPE_MEASUREMENT_REPORT_TYPE_NUM; i++) {
            if ((active_map >> i) & 1) {
                active_item_cnt++;
            }
        }
        notify_item_cnt = BASIC_NOTIF_ITEM_CNT + sensor_cnt * (active_item_cnt + 1); // 3 + 1 for measurement-object, start-time, end-time
    } else {
        notify_item_cnt = TX_RX_WIN_NOTIF_ITEM_CNT;
    }

    /* notif_val is freed one by one, so it need to be allocated one by one. */
    *notif_val = (sr_val_t**)calloc(notify_item_cnt, sizeof(sr_val_t*));
    notif_val_list = *notif_val;
    for (size_t i = 0; i < notify_item_cnt; i++) {
        notif_val_list[i] = (sr_val_t*)calloc(1, sizeof(sr_val_t));
        if (notif_val_list[i] == NULL) {
            OAM_LOG_ERROR("can't alloc mem for notif_val");
            for (size_t j = 0; j < i; j++) {
                free(notif_val_list[j]);
            }
            return -1;
        }
    }

    return notify_item_cnt;
}

static void fill_notification_common_data(sr_val_t** notif_val, const char* meas_grp_name, const char* meas_obj_name, uint16_t interval) // Fill common data for notifications (3 items)
{
    time_t now;
    char start_time_str[LOCAL_TIME_STR_LEN];
    char end_time_str[LOCAL_TIME_STR_LEN];
    char* time_str = NULL;
    char xpath[XPATH_LEN];
    char notif_xpath[] = "/o-ran-performance-management:measurement-result-stats";

    if (strcmp(meas_grp_name, "epe") == 0) {
        snprintf(xpath, sizeof(xpath), "%s/%s-statistics[measurement-object='%s']/measurement-object", notif_xpath, meas_grp_name, meas_obj_name);
    } else {
        snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/measurement-object", notif_xpath, meas_grp_name, meas_obj_name);
    }

    notif_val[0]->xpath = strdup(xpath);
    notif_val[0]->type = SR_ENUM_T;
    notif_val[0]->data.enum_val = strdup(meas_obj_name);

    now = time(NULL);
    ly_time_time2str(now - interval, NULL, &time_str);
    strncpy(start_time_str, time_str, sizeof(start_time_str));
    free(time_str);
    notif_val[1]->type = SR_STRING_T;
    notif_val[1]->data.string_val = strdup(start_time_str);

    ly_time_time2str(now, NULL, &time_str);
    strncpy(end_time_str, time_str, sizeof(end_time_str));
    free(time_str);
    notif_val[2]->type = SR_STRING_T;
    notif_val[2]->data.string_val = strdup(end_time_str);
}

static int write_epe_notification_data(epe_measurement_object_e epe_measurement_object)  // Write EPE measurement notification data
{
    sr_val_t** notif_val = NULL;
    uint32_t notify_item_cnt = 0;
    uint32_t report_index = BASIC_NOTIF_ITEM_CNT;
    char xpath[XPATH_LEN];
    char notif_xpath[] = "/o-ran-performance-management:measurement-result-stats";
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();
    epe_measurement_result_t* epe_result = NULL;
    uint32_t index = ++(perf_mgmt_ctx->notif_ctx.latest_notif[EPE_MEASUREMENT][epe_measurement_object].index);
    const char* meas_grp_name = get_meas_grp_name(EPE_MEASUREMENT);
    const char* meas_obj_name = get_meas_obj_name(EPE_MEASUREMENT, epe_measurement_object);
    char** multiple_result_xpath = NULL;

    for (size_t i = 0; i < perf_mgmt_ctx->epe_measurement_ctx.epe_result_list_num; i++) {
        epe_result = &perf_mgmt_ctx->epe_measurement_ctx.epe_result_list[i];
        if (((epe_measurement_object == TEMPERATURE) && (epe_result->sensor_type != SENSOR_TYPE_TEMPERATURE))) {
            continue;
        } else if (((epe_measurement_object != TEMPERATURE) && (epe_result->sensor_type != SENSOR_TYPE_POWER_RAIL))) {
            continue;
        }

        if (notif_val == NULL) {
            notify_item_cnt = alloc_memory_for_notification_data(perf_mgmt_ctx, &notif_val, EPE_MEASUREMENT, epe_measurement_object, epe_result->active_map);  // Allocate memory for notification data
            if (notify_item_cnt < 0) {
                return SR_ERR_OPERATION_FAILED;
            }
            fill_notification_common_data(notif_val, meas_grp_name, meas_obj_name, perf_mgmt_ctx->epe_measurement_ctx.interval);
            multiple_result_xpath = malloc((notify_item_cnt - 1) * sizeof(char*));
            snprintf(xpath, sizeof(xpath), "%s/epe-statistics[measurement-object='%s']/start-time", notif_xpath, meas_obj_name);  // notif_xpath[] = "/o-ran-performance-management:measurement-result-stats";
            notif_val[1]->xpath = strdup(xpath);
            snprintf(xpath, sizeof(xpath), "%s/epe-statistics[measurement-object='%s']/end-time", notif_xpath, meas_obj_name); // otif_xpath[] = "/o-ran-performance-management:measurement-result-stats";
            notif_val[2]->xpath = strdup(xpath);
            snprintf(xpath, sizeof(xpath), "%s/epe-statistics[measurement-object='%s']/multiple-epe-measurement-result[%u]/start-time", notif_xpath, meas_obj_name, index);  // notif_xpath[] = "/o-ran-performance-management:measurement-result-stats";
            multiple_result_xpath[0] = strdup(xpath);
            snprintf(xpath, sizeof(xpath), "%s/epe-statistics[measurement-object='%s']/multiple-epe-measurement-result[%u]/end-time", notif_xpath, meas_obj_name, index);  // notif_xpath[] = "/o-ran-performance-management:measurement-result-stats";
            multiple_result_xpath[1] = strdup(xpath);
        }

        notif_val[report_index]->type = SR_STRING_T;
        notif_val[report_index]->data.string_val = strdup(epe_result->sensor_name);
        snprintf(xpath, sizeof(xpath), "%s/epe-statistics[measurement-object='%s']/epe-measurement-resultv2[object-unit-id='%s']/object-unit-id", notif_xpath, meas_obj_name,
            epe_result->sensor_name);  //notif_xpath[] = "/o-ran-performance-management:measurement-result-stats";
        notif_val[report_index]->xpath = strdup(xpath);
        snprintf(xpath, sizeof(xpath),
            "%s/epe-statistics[measurement-object='%s']/multiple-epe-measurement-result[%u]/epe-measurement-resultv2[object-unit-id='%s']/object-unit-id", notif_xpath,
            meas_obj_name, index, epe_result->sensor_name);  //notif_xpath[] = "/o-ran-performance-management:measurement-result-stats";
        multiple_result_xpath[report_index - 1] = strdup(xpath);
        report_index++;
        for (size_t j = 0; j < EPE_MEASUREMENT_REPORT_TYPE_NUM; j++) {
            if ((epe_result->active_map >> j) & 1) {
                notif_val[report_index]->type = SR_DECIMAL64_T;
                notif_val[report_index]->data.decimal64_val = epe_result->monitor_result[epe_measurement_object][j];
                snprintf(xpath, sizeof(xpath), "%s/epe-statistics[measurement-object='%s']/epe-measurement-resultv2[object-unit-id='%s']/%s", notif_xpath, meas_obj_name,
                    epe_result->sensor_name, get_epe_measurement_result_leaf_str(j));
                notif_val[report_index]->xpath = strdup(xpath);
                snprintf(xpath, sizeof(xpath), "%s/epe-statistics[measurement-object='%s']/multiple-epe-measurement-result[%u]/epe-measurement-resultv2[object-unit-id='%s']/%s",
                    notif_xpath, meas_obj_name, index, epe_result->sensor_name, get_epe_measurement_result_leaf_str(j));
                multiple_result_xpath[report_index - 1] = strdup(xpath);
                report_index++;
            }
        }
    }
    write_latest_notif_data(EPE_MEASUREMENT, epe_measurement_object, notif_val, notify_item_cnt);  // Write the latest notification data

    for (size_t i = 0; i < notify_item_cnt - 1; i++) {
        notif_val[i + 1]->xpath = multiple_result_xpath[i];
    }
    write_multi_notif_data(perf_mgmt_ctx, notif_val, notify_item_cnt);  // Write multiple notification data
    free(notif_val);
    free(multiple_result_xpath);

    return SR_ERR_OK;
}
static int write_epe_meas_notification(sr_session_ctx_t* running_session)
{
    int rc = SR_ERR_OK;
    uint32_t epe_meas_obj_id = 0;
    sr_val_t* values = NULL;
    size_t values_count = 0;

    rc = sr_get_items(running_session, "/o-ran-performance-management:performance-measurement-objects/epe-measurement-objects/active", 0, 0, &values, &values_count);
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of epe-measurement-objects \"active\"");
        goto error;
    }

    for (size_t i = 0; i < values_count; i++) {
        if (!values[i].data.bool_val) {
            continue;
        }

        epe_meas_obj_id = (uint32_t)get_meas_obj_id(strchr(values[i].xpath, '\'') + 1, EPE_MEASUREMENT);  // Get the measurement object ID
        rc = write_epe_notification_data(epe_meas_obj_id);  // Write EPE measurement notification data
        if (rc != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to poll epe-measurement report info");
            goto error;
        }
    }
    sr_free_values(values, values_count);
    return rc;

error:
    sr_free_values(values, values_count);
    return rc;
}

static inline sr_val_t* alloc_one_notif_val()
{
    sr_val_t* notif_val = (sr_val_t*)calloc(1, sizeof(sr_val_t));
    if (notif_val == NULL) {
        OAM_LOG_ERROR("can't alloc mem for notif_val");
        return NULL;
    }
    return notif_val;
}

static int write_transceiver_notif(measurement_group_e meas_grp, uint32_t meas_obj_id, uint16_t interval, transceiver_result_info_t* transceiver_result)
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();
    uint32_t index = ++(perf_mgmt_ctx->notif_ctx.latest_notif[meas_grp][meas_obj_id].index);
    sr_val_t** notif_val = calloc(TRANSCEIVER_NOTIF_ITEM_CNT + transceiver_result->bin_count, sizeof(sr_val_t*));
    uint16_t i = 0;

    char xpath[XPATH_LEN];
    char notif_xpath[] = "/o-ran-performance-management:measurement-result-stats";
    const char* meas_grp_name = get_meas_grp_name(meas_grp);
    const char* meas_obj_name = get_meas_obj_name(meas_grp, meas_obj_id);
    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/measurement-object", notif_xpath, meas_grp_name, meas_obj_name);
    notif_val[0] = alloc_one_notif_val();
    if (notif_val[0] == NULL) {
        free(notif_val);
        return -1;
    }
    notif_val[0]->xpath = strdup(xpath);
    notif_val[0]->type = SR_ENUM_T;
    notif_val[0]->data.enum_val = strdup(meas_obj_name);

    char* time_str = NULL;
    time_t now = time(NULL);
    ly_time_time2str(now - interval, NULL, &time_str);
    char start_time_str[LOCAL_TIME_STR_LEN];
    strncpy(start_time_str, time_str, sizeof(start_time_str));
    free(time_str);
    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/start-time", notif_xpath, meas_grp_name, meas_obj_name);
    notif_val[1] = alloc_one_notif_val();
    if (notif_val[1] == NULL) {
        free(notif_val[0]);
        free(notif_val);
        return -1;
    }
    notif_val[1]->xpath = strdup(xpath);
    notif_val[1]->type = SR_STRING_T;
    notif_val[1]->data.string_val = strdup(start_time_str);

    char end_time_str[LOCAL_TIME_STR_LEN];
    ly_time_time2str(now, NULL, &time_str);
    strncpy(end_time_str, time_str, sizeof(end_time_str));
    free(time_str);
    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/end-time", notif_xpath, meas_grp_name, meas_obj_name);
    notif_val[2] = alloc_one_notif_val();
    if (notif_val[2] == NULL) {
        free(notif_val[0]);
        free(notif_val[1]);
        free(notif_val);
        return -1;
    }
    notif_val[2]->xpath = strdup(xpath);
    notif_val[2]->type = SR_STRING_T;
    notif_val[2]->data.string_val = strdup(end_time_str);

    i = BASIC_NOTIF_ITEM_CNT;
    if (transceiver_result->min_active) {
        snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/transceiver-measurement-result[object-unit-id='0']/min/value", notif_xpath, meas_grp_name,
            meas_obj_name);
        notif_val[i] = alloc_one_notif_val();
        if (notif_val[i] == NULL) {
            goto error;
        }
        notif_val[i]->xpath = strdup(xpath);
        notif_val[i]->type = SR_DECIMAL64_T;
        notif_val[i]->data.decimal64_val = transceiver_result->min;
        i += 1;
        if (strlen(transceiver_result->min_time) > 0) {
            snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/transceiver-measurement-result[object-unit-id='0']/min/time", notif_xpath, meas_grp_name,
                meas_obj_name);
            notif_val[i] = alloc_one_notif_val();
            if (notif_val[i] == NULL) {
                goto error;
            }
            notif_val[i]->xpath = strdup(xpath);
            notif_val[i]->type = SR_STRING_T;
            notif_val[i]->data.string_val = strdup(transceiver_result->min_time);
            i += 1;
        }
    }
    if (transceiver_result->max_active) {
        snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/transceiver-measurement-result[object-unit-id='0']/max/value", notif_xpath, meas_grp_name,
            meas_obj_name);
        notif_val[i] = alloc_one_notif_val();
        if (notif_val[i] == NULL) {
            goto error;
        }
        notif_val[i]->xpath = strdup(xpath);
        notif_val[i]->type = SR_DECIMAL64_T;
        notif_val[i]->data.decimal64_val = transceiver_result->max;
        i += 1;
        if (strlen(transceiver_result->max_time) > 0) {
            snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/transceiver-measurement-result[object-unit-id='0']/max/time", notif_xpath, meas_grp_name,
                meas_obj_name);
            notif_val[i] = alloc_one_notif_val();
            if (notif_val[i] == NULL) {
                goto error;
            }
            notif_val[i]->xpath = strdup(xpath);
            notif_val[i]->type = SR_STRING_T;
            notif_val[i]->data.string_val = strdup(transceiver_result->max_time);
            i += 1;
        }
    }
    if (transceiver_result->first_active) {
        snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/transceiver-measurement-result[object-unit-id='0']/first/value", notif_xpath, meas_grp_name,
            meas_obj_name);
        notif_val[i] = alloc_one_notif_val();
        if (notif_val[i] == NULL) {
            goto error;
        }
        notif_val[i]->xpath = strdup(xpath);
        notif_val[i]->type = SR_DECIMAL64_T;
        notif_val[i]->data.decimal64_val = transceiver_result->first;
        i += 1;
        if (strlen(transceiver_result->first_time) > 0) {
            snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/transceiver-measurement-result[object-unit-id='0']/first/time", notif_xpath, meas_grp_name,
                meas_obj_name);
            notif_val[i] = alloc_one_notif_val();
            if (notif_val[i] == NULL) {
                goto error;
            }
            notif_val[i]->xpath = strdup(xpath);
            notif_val[i]->type = SR_STRING_T;
            notif_val[i]->data.string_val = strdup(transceiver_result->first_time);
            i += 1;
        }
    }
    if (transceiver_result->latest_active) {
        snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/transceiver-measurement-result[object-unit-id='0']/latest/value", notif_xpath, meas_grp_name,
            meas_obj_name);
        notif_val[i] = alloc_one_notif_val();
        if (notif_val[i] == NULL) {
            goto error;
        }
        notif_val[i]->xpath = strdup(xpath);
        notif_val[i]->type = SR_DECIMAL64_T;
        notif_val[i]->data.decimal64_val = transceiver_result->latest;
        i += 1;
        if (strlen(transceiver_result->latest_time) > 0) {
            snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/transceiver-measurement-result[object-unit-id='0']/latest/time", notif_xpath, meas_grp_name,
                meas_obj_name);
            notif_val[i] = alloc_one_notif_val();
            if (notif_val[i] == NULL) {
                goto error;
            }
            notif_val[i]->xpath = strdup(xpath);
            notif_val[i]->type = SR_STRING_T;
            notif_val[i]->data.string_val = strdup(transceiver_result->latest_time);
            i += 1;
        }
    }
    for (uint16_t j = 0; j < transceiver_result->bin_count; j++) {
        snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/transceiver-measurement-result[object-unit-id='0']/frequency-bin-table[bin-id='%u']/value",
            notif_xpath, meas_grp_name, meas_obj_name, j);
        notif_val[i] = alloc_one_notif_val();
        if (notif_val[i] == NULL) {
            goto error;
        }
        notif_val[i]->xpath = strdup(xpath);
        notif_val[i]->type = SR_UINT32_T;
        notif_val[i]->data.uint32_val = transceiver_result->freq_bin_table[j];
        i += 1;
    }
    uint8_t notif_value_cnt = i;
    write_latest_notif_data(meas_grp, meas_obj_id, notif_val, notif_value_cnt);

    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/start-time", notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name,
        index);
    notif_val[1]->xpath = strdup(xpath);
    snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/end-time", notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name,
        index);
    notif_val[2]->xpath = strdup(xpath);
    i = BASIC_NOTIF_ITEM_CNT;
    if (transceiver_result->min_active) {
        snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/transceiver-measurement-result[object-unit-id='0']/min/value",
            notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name, index);
        notif_val[i]->xpath = strdup(xpath);
        i += 1;
        if (strlen(transceiver_result->min_time) > 0) {
            snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/transceiver-measurement-result[object-unit-id='0']/min/time",
                notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name, index);
            notif_val[i]->xpath = strdup(xpath);
            i += 1;
        }
    }
    if (transceiver_result->max_active) {
        snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/transceiver-measurement-result[object-unit-id='0']/max/value",
            notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name, index);
        notif_val[i]->xpath = strdup(xpath);
        i += 1;
        if (strlen(transceiver_result->max_time) > 0) {
            snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/transceiver-measurement-result[object-unit-id='0']/max/time",
                notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name, index);
            notif_val[i]->xpath = strdup(xpath);
            i += 1;
        }
    }
    if (transceiver_result->first_active) {
        snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/transceiver-measurement-result[object-unit-id='0']/first/value",
            notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name, index);
        notif_val[i]->xpath = strdup(xpath);
        i += 1;
        if (strlen(transceiver_result->first_time) > 0) {
            snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/transceiver-measurement-result[object-unit-id='0']/first/time",
                notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name, index);
            notif_val[i]->xpath = strdup(xpath);
            i += 1;
        }
    }
    if (transceiver_result->latest_active) {
        snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/transceiver-measurement-result[object-unit-id='0']/latest/value",
            notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name, index);
        notif_val[i]->xpath = strdup(xpath);
        i += 1;
        if (strlen(transceiver_result->latest_time) > 0) {
            snprintf(xpath, sizeof(xpath), "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/transceiver-measurement-result[object-unit-id='0']/latest/time",
                notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name, index);
            notif_val[i]->xpath = strdup(xpath);
            i += 1;
        }
    }
    for (uint16_t j = 0; j < transceiver_result->bin_count; j++) {
        snprintf(xpath, sizeof(xpath),
            "%s/%s-stats[measurement-object='%s']/multiple-%s-measurement-result[%u]/transceiver-measurement-result[object-unit-id='0']/frequency-bin-table[bin-id='%u']/value",
            notif_xpath, meas_grp_name, meas_obj_name, meas_grp_name, index, j);
        notif_val[i]->xpath = strdup(xpath);
        i += 1;
    }
    write_multi_notif_data(perf_mgmt_ctx, notif_val, notif_value_cnt);

    free(notif_val);
    return 0;

error:
    for (uint16_t j = 0; j <= i; j++) {
        free(notif_val[j]);
    }
    free(notif_val);
    return -1;
}

static int write_transceiver_meas_notification(perf_mgmt_transceiver_measurement_ctx_t* ctx, transceiver_result_info_t* transceiver_result)
{
    if (transceiver_result == NULL) {
        return SR_ERR_OPERATION_FAILED;
    }
    int ret = SR_ERR_OK;
    for (uint32_t meas_obj_id = 0; meas_obj_id < TRANSCEIVER_MEASUREMENT_OBJECT_NUM; meas_obj_id++) {
        if (transceiver_result[meas_obj_id].active) {
            write_transceiver_notif(TRANSCEIVER_MEASUREMENT, meas_obj_id, ctx->interval, &transceiver_result[meas_obj_id]);
        }
    }

    return ret;
}

static int perf_mgmt_write_notification(sr_session_ctx_t* running_session, perf_mgmt_measurement_result_t* measurement_result)  // write notification data
{
    int ret = SR_ERR_OK;
    ret = check_notif_enabled();
    if (ret != 0) {
        return ret;
    }

    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();
    pthread_mutex_lock(&perf_mgmt_ctx->notif_ctx.notif_mutex);
    switch (measurement_result->measurement_group) {
        case TX_MEASUREMENT: {
            ret = write_tx_meas_notification_unit_RU(running_session, measurement_result->counter_result);
            break;
        }

        case RX_WINDOW_MEASUREMENT: {
            ret = write_rx_win_meas_notification_unit_RU(running_session, measurement_result->counter_result);
            break;
        }

        case EPE_MEASUREMENT: {
            ret = write_epe_meas_notification(running_session);
            break;
        }

        case TRANSCEIVER_MEASUREMENT: {
            ret = write_transceiver_meas_notification(&perf_mgmt_ctx->transceiver_meas_ctx, measurement_result->transceiver_result);
            break;
        }

        case SYMBOL_RSSI_MEASUREMENT:
        case NUM_OF_MEASUREMENT:
        default:
            break;
    }

    pthread_mutex_unlock(&perf_mgmt_ctx->notif_ctx.notif_mutex);
    return ret;
}

void perf_mgmt_handle_measurement_counter_result_ind(measurement_counter_result_ind_t* result)
{
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* running_session = NULL;
    sr_session_ctx_t* operational_session = NULL;
    int rc = sr_session_start(oam_netconf_ctx->netconf_ds_conn, SR_DS_RUNNING, &running_session);
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("fail to getting running session when handling measurement_counter_result_ind_t: %s", sr_strerror(rc));
        goto cleanup;
    }
    rc = sr_session_start(oam_netconf_ctx->netconf_ds_conn, SR_DS_OPERATIONAL, &operational_session);
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("fail to getting operational session when handling measurement_counter_result_ind_t: %s", sr_strerror(rc));
        goto cleanup;
    }

    perf_mgmt_measurement_result_t measurement_result;
    measurement_result.measurement_group = result->measurement_group;
    measurement_result.measurement_interval = result->measurement_interval;
    measurement_result.counter_result = result;

    // fill latest measurement data
    perf_mgmt_fill_meas_result(running_session, operational_session, &measurement_result);

    // write data into file
    perf_mgmt_write_upload_file(running_session, &measurement_result);

    // write notification data
    perf_mgmt_write_notification(running_session, &measurement_result);
cleanup:
    if (running_session) {
        sr_session_stop(running_session);
    }
    if (operational_session) {
        sr_session_stop(operational_session);
    }
}

/********************************************<callback function>*************************************************/

static void fill_meas_msg(measurement_status_update_req_list_t* message, uint8_t msg_index, uint16_t grp_id, uint16_t meas_itv, uint8_t active, uint8_t object_unit,
    uint8_t report_info)
{
    measurement_status_update_req_t* status_update_req = &message->measurement_status_update_msg[msg_index];

    status_update_req->measurement_group = grp_id;
    status_update_req->measurement_interval = meas_itv;
    status_update_req->active = active;
    status_update_req->object_unit = object_unit;
    status_update_req->report_info = report_info;

    message->num_of_msg = msg_index + 1;
}

static int meas_obj_str2num(const char* meas_obj_str, int* grp_id, int* obj_id)
{
    for (int tmp_grp_id = 0; tmp_grp_id < SYMBOL_RSSI_MEASUREMENT; tmp_grp_id++) {
        for (int tmp_obj_id = 0; tmp_obj_id < MAX_MEAS_OBJ_CNT; tmp_obj_id++) {
            char* measurement_object = get_meas_obj_name(tmp_grp_id, tmp_obj_id);
            if (measurement_object && (!strcmp(meas_obj_str, measurement_object))) {
                *grp_id = tmp_grp_id;
                *obj_id = tmp_obj_id;
                return 0;
            }
        }
    }

    return -1;
}

static int get_meas_interval(sr_session_ctx_t* session, char* meas_grp, uint16_t* measurement_interval)
{
    int ret = SR_ERR_OK;
    sr_val_t* values = NULL;
    size_t values_count = 0;
    char path[XPATH_LEN] = { '\0' };
    char root_path[] = "/o-ran-performance-management:performance-measurement-objects";

    snprintf(path, sizeof(path), "%s/%s-interval", root_path, meas_grp);
    ret = sr_get_items(session, path, 0, 0, &values, &values_count);
    if (ret != SR_ERR_OK) {
        OAM_LOG_ERROR("can't get interval value of %s-measurement-objects (%s)", meas_grp, sr_strerror(ret));
    } else if (values_count == 0) {
        OAM_LOG_INFO("there is no interval value of %s-measurement-objects", meas_grp);
        return -1;
    } else {
        *measurement_interval = values[0].data.uint16_val;
    }

    sr_free_values(values, values_count);
    return ret;
}

/* Check if measurement interval of supported measurement groups are same */
static int oam_check_meas_itv(sr_session_ctx_t* session)
{
    int ret = 0;
    size_t i;
    size_t grp_cnt = sizeof(gSupportMeasGroupName) / sizeof(gSupportMeasGroupName[0]);
    char meas_grp_name[32];
    uint16_t old_meas_itv = 0xffff, new_meas_itv;

    for (i = 0; i < grp_cnt; i++) {
        if (i == EPE_MEASUREMENT || i == TRANSCEIVER_MEASUREMENT) {
            continue;
        }

        snprintf(meas_grp_name, sizeof(meas_grp_name), "%s-measurement", gSupportMeasGroupName[i]);
        ret = get_meas_interval(session, meas_grp_name, &new_meas_itv);
        if (ret == SR_ERR_OK) {
            if (old_meas_itv != 0xffff && old_meas_itv != new_meas_itv) {
                return -1;
            }
            old_meas_itv = new_meas_itv;
        }
    }
    return 0;
}

/**
 * @brief When a tx/rx measurement object is set to inactive, reset its measurement result in sysrepo.
 */
static int perf_mgmt_reset_meas_result(sr_session_ctx_t* session, int grp_id, int obj_id)
{
    if (grp_id != TX_MEASUREMENT && grp_id != RX_WINDOW_MEASUREMENT) {
        return SR_ERR_OK;
    }
    const char* group_name = get_meas_grp_name(grp_id);
    const char* meas_obj_name = get_meas_obj_name(grp_id, obj_id);
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* sess_for_oper = oam_netconf_ctx->netconf_ds_sess_operational;

    sr_val_t* value = NULL;
    char xpath[XPATH_LEN];
    char root_xpath[] = "/o-ran-performance-management:performance-measurement-objects";

    snprintf(xpath, sizeof(xpath), "%s/%s-measurement-objects[measurement-object='%s']/active", root_xpath, group_name, meas_obj_name);
    int ret = sr_get_item(session, xpath, 0, &value);
    if (ret != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of %s, %s", xpath, sr_strerror(ret));
        goto error;
    }

    if (!value->data.bool_val) {
        OAM_LOG_DEBUG("as group [%s] object [%s] become inactive, resetting measurement counter to 0", group_name, meas_obj_name);
        snprintf(xpath, sizeof(xpath), "%s/%s-measurement-objects[measurement-object='%s']/count", root_xpath, group_name, meas_obj_name);
        ret = sr_set_item_str(sess_for_oper, xpath, "0", NULL, 0);
        if (ret != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to set the value of %s, %s", xpath, sr_strerror(ret));
            goto error;
        }
        ret = sr_apply_changes(sess_for_oper, 0);
        if (ret != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to apply measurement counter reset");
            goto error;
        }
    }

error:
    sr_free_val(value);
    return ret;
}

static int perf_mgmt_get_config_change(sr_session_ctx_t* session, uint64_t* obj_change_map, uint8_t* itv_change_map)
{
    int ret = SR_ERR_OK;
    sr_change_iter_t* iter = NULL;
    sr_change_oper_t op;
    const struct lyd_node* node;
    int grp_id = 0, obj_id = 0;

    ret = oam_check_meas_itv(session);
    if (ret == -1) {
        OAM_LOG_ERROR("measurement interval of supported measurement group should be same");
        return SR_ERR_INVAL_ARG;
    }

    ret = sr_get_changes_iter(session, "/o-ran-performance-management:performance-measurement-objects/*//.", &iter);
    if (ret != SR_ERR_OK) {
        OAM_LOG_ERROR("sr_get_changes_iter failed");
        return ret;
    }

    while ((ret = sr_get_change_tree_next(session, iter, &op, &node, NULL, NULL, NULL)) == SR_ERR_OK) {
        OAM_LOG_DEBUG("performance measurement object node '%s' changed", node->schema->name);
        if (!strcmp(node->schema->name, "active") || !strcmp(node->schema->name, "object-unit") || !strcmp(node->schema->name, "report-info")) {
            const char* meas_obj_str = lyd_get_value(node->parent->child);
            if (-1 == meas_obj_str2num(meas_obj_str, &grp_id, &obj_id)) {
                OAM_LOG_ERROR("unsupported measurement group/object: %s", meas_obj_str);
                return SR_ERR_INVAL_ARG;
            }
            *obj_change_map |= 1 << (grp_id * MAX_MEAS_OBJ_CNT + obj_id);
            // some object become inactive, reset the counter
            perf_mgmt_reset_meas_result(session, grp_id, obj_id);
            continue;
        }
        if (!strcmp(node->schema->name, "tx-measurement-interval")) {
            *itv_change_map |= (1 << TX_MEASUREMENT);
            continue;
        }
        if (!strcmp(node->schema->name, "rx-window-measurement-interval")) {
            *itv_change_map |= (1 << RX_WINDOW_MEASUREMENT);
            continue;
        }

        if (!strcmp(node->schema->name, "epe-measurement-interval")) {
            uint16_t interval = (uint16_t)strtoul(lyd_get_value(node), NULL, 0);
            if (interval < ENVIRONMENTAL_SAMPLE_INTERVAL) {
                sr_session_set_error_message(session, "The epe-measurement-interval must be longer than 1s and 5s.");
                return SR_ERR_INVAL_ARG;
            }

            continue;
        }

        if (!strcmp(node->schema->name, "bin-count")) {
            uint16_t bin_count = (uint16_t)strtoul(lyd_get_value(node), NULL, 0);
            sr_session_ctx_t* sess_for_oper = get_oam_netconf_ctx()->netconf_ds_sess_operational;
            sr_val_t* val;
            ret = sr_get_item(sess_for_oper, "/o-ran-performance-management:performance-measurement-objects/max-bin-count", 0, &val);
            if (ret != SR_ERR_OK) {
                OAM_LOG_ERROR("failed to get the value of max-bin-count of performance management (%s)", sr_strerror(ret));
                return ret;
            }

            uint16_t max_bin_count = val->data.uint16_val;
            sr_free_val(val);

            if (bin_count > max_bin_count) {
                sr_session_set_error_message(session, "The bin-count must be less than or equal to %u.", max_bin_count);
                return SR_ERR_INVAL_ARG;
            }
        }
    }
    return 0;
}

static int get_meas_obj_data(sr_session_ctx_t* session, char* meas_grp, uint8_t* rst_active, uint8_t* rst_object_unit, uint8_t* rst_report_info)
{
    int ret = SR_ERR_OK;
    uint8_t active_enable_flag = 0;
    sr_val_t* values = NULL;
    size_t values_count = 0;
    char path[XPATH_LEN] = { '\0' };
    char root_path[] = "/o-ran-performance-management:performance-measurement-objects";

    snprintf(path, sizeof(path), "%s/%s-objects/active", root_path, meas_grp);
    ret = sr_get_items(session, path, 0, 0, &values, &values_count);
    if (ret != SR_ERR_OK && ret != SR_ERR_NOT_FOUND) {
        OAM_LOG_ERROR("can't get the value of %s-objects \"active\"", meas_grp);
        goto error;
    }
    for (size_t i = 0; i < values_count; i++) {
        if (values[i].data.bool_val) {
            active_enable_flag = 1;
            break;
        }
    }

    *rst_active = active_enable_flag;
    /* these two values are fixed now */
    *rst_object_unit = 0;
    *rst_report_info = 0;

    return 0;

error:
    sr_free_values(values, values_count);
    return ret;
}

static int perf_mgmt_send_msg_while_config_change(sr_session_ctx_t* session, uint64_t obj_change_map, uint8_t itv_change_map)  // Send measurement status update request message
{
    uint8_t msg_cnt = 0;

    measurement_status_update_req_list_t* message = calloc(1, sizeof(*message));  // Allocate memory for the message
    if (message == NULL) {
        OAM_LOG_ERROR("fail to alloc memory size %d", sizeof(*message));
        return SR_ERR_NO_MEMORY;
    }

    char* support_measurement[] = { "tx-measurement", "rx-window-measurement" };

    for (uint16_t i = 0; i < sizeof(support_measurement) / sizeof(support_measurement[0]); i++) {  // group id 0 and 1 are tx and rx measurement respectively
        if (((itv_change_map >> i) & 0b01) || ((obj_change_map >> (i * MAX_MEAS_OBJ_CNT)) & MEAS_OBJ_MAP)) {  // If measurement interval or object changed
            uint16_t measurement_interval = 0;

            int ret = get_meas_interval(session, support_measurement[i], &measurement_interval);
            if (ret != -1 && ret != SR_ERR_OK) {
                return -1;
            }
            uint8_t report_info = 0;
            uint8_t object_unit = 0;
            uint8_t active = 0;
            ret = get_meas_obj_data(session, support_measurement[i], &active, &object_unit, &report_info);
            if (ret != SR_ERR_NOT_FOUND && ret != SR_ERR_OK) {
                return -1;
            }
            fill_meas_msg(message, msg_cnt, i, measurement_interval, active, object_unit, report_info); // get measurement group id, interval, active status, object unit and report info
            msg_cnt++;
        }
    }
    bool success = send_internal_message_and_wait_done(INTERNAL_MESSAGE_SEND_MEASUREMENT_STATUS_UPDATE_REQ, NULL, 0, message);  // Send the message to the internal message queue
    if (!success) {
        OAM_LOG_ERROR("send_measurement_status_update_req_list failed");
    }
    free(message);
    return success ? SR_ERR_OK : SR_ERR_OPERATION_FAILED;
}

/** Check if any measurement object is active.
 * If none, return 0.
 * If any, return 1.
 */
static int check_any_active_meas_obj_exist()
{
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* session = oam_netconf_ctx->netconf_ds_sess_operational;

    sr_val_t* values = NULL;
    size_t values_count = 0;
    char xpath[XPATH_LEN];

    snprintf(xpath, sizeof(xpath), "/o-ran-performance-management:performance-measurement-objects/*/active");
    int ret = sr_get_items(session, xpath, 0, 0, &values, &values_count);
    if (ret != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of any measurement object");
        return 0;
    }

    for (size_t i = 0; i < values_count; i++) {
        if (values[i].data.bool_val) {
            sr_free_values(values, values_count);
            return 1;
        }
    }
    sr_free_values(values, values_count);
    return 0;
}

/** Check whether any measurement object in the input measurement group is active.
 * If none, return 0.
 * If any, return 1.
 */
static int check_active_meas_obj_exist(measurement_group_e meas_grp)
{
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* session = oam_netconf_ctx->netconf_ds_sess_operational;

    sr_val_t* values = NULL;
    size_t values_count = 0;
    char xpath[XPATH_LEN];

    snprintf(xpath, sizeof(xpath), "/o-ran-performance-management:performance-measurement-objects/%s-measurement-objects/active", get_meas_grp_name(meas_grp)); //epe, transceiver, tx, rx
    int ret = sr_get_items(session, xpath, 0, 0, &values, &values_count);
    if (ret != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of %s measurement objects", get_meas_grp_name(meas_grp));
        return 0;
    }

    for (size_t i = 0; i < values_count; i++) {
        if (values[i].data.bool_val) {
            sr_free_values(values, values_count);
            return 1;
        }
    }
    sr_free_values(values, values_count);
    return 0;
}

static void file_upload_interval_changed()
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();

    perf_mgmt_ctx->file_upload_ctx.file_upload_mode = get_file_upload_mode();
    perf_mgmt_ctx->file_upload_ctx.file_upload_interval = get_perf_mgmt_interval("file-upload");
    OAM_LOG_DEBUG("file upload mode enabled: %d, upload interval: %d", perf_mgmt_ctx->file_upload_ctx.file_upload_mode, perf_mgmt_ctx->file_upload_ctx.file_upload_interval);
    if (perf_mgmt_ctx->file_upload_ctx.file_upload_mode == 0) {
        return;
    }
    if (perf_mgmt_ctx->file_upload_ctx.file_upload_interval == 0) {
        return;
    }

    if (check_any_active_meas_obj_exist() == 0) {
        OAM_LOG_WARN("file update is enabled, but there is no any active measurement objects configured");
        perf_mgmt_ctx->file_upload_ctx.file_upload_mode = 0;
        return;
    }

    if (!perf_mgmt_ctx->file_upload_ctx.file_upload_tid) {
        pthread_create((pthread_t*)&perf_mgmt_ctx->file_upload_ctx.file_upload_tid, NULL, perf_mgmt_upload_curr_file_thread, (void*)perf_mgmt_ctx);
        OAM_LOG_INFO("file upload thread created: %u", perf_mgmt_ctx->file_upload_ctx.file_upload_tid);
    }
}

static void perf_mgmt_free_multi_notif_data(perf_mgmt_notif_data_t* perf_mgmt_notif_data)
{
    if (!perf_mgmt_notif_data) {
        return;
    }

    if (perf_mgmt_notif_data->values) {
        sr_free_val(perf_mgmt_notif_data->values);
        perf_mgmt_notif_data->values = NULL;
    }

    if (perf_mgmt_notif_data->next) {
        perf_mgmt_free_multi_notif_data(perf_mgmt_notif_data->next);
        perf_mgmt_notif_data->next = NULL;
    }
}

static void perf_mgmt_free_multi_notif_meas_stats(perf_mgmt_measurement_stats_t* perf_mgmt_measurement_stats)
{
    if (perf_mgmt_measurement_stats->data_head) {
        perf_mgmt_free_multi_notif_data(perf_mgmt_measurement_stats->data_head);
        perf_mgmt_measurement_stats->data_head = NULL;
        perf_mgmt_measurement_stats->data_end = NULL;
        perf_mgmt_measurement_stats->data_cnt = 0;
    }
}

static void perf_mgmt_free_all_notif_data(oam_perf_mgmt_ctx_t* perf_mgmt_ctx)
{
    perf_mgmt_latest_notif_t* latest = NULL;
    for (size_t i = 0; i < USED_MEAS_GRP_CNT; i++) {
        for (size_t j = 0; j < MAX_MEAS_OBJ_CNT; j++) {
            latest = &perf_mgmt_ctx->notif_ctx.latest_notif[i][j];
            if (latest->active == 1) {
                latest->index = 0;
                /* free xpaths specifically allocated for latest notification. */
                for (size_t m = 1; m < latest->data_cnt; m++) {
                    free(latest->data[m].xpath);
                }
                latest->active = 0;
            }
        }
    }
    perf_mgmt_free_multi_notif_meas_stats(&perf_mgmt_ctx->notif_ctx.measurement_stats);
}

static void perf_mgmt_send_notif(oam_perf_mgmt_ctx_t* perf_mgmt_ctx)
{
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* session = oam_netconf_ctx->netconf_ds_sess_operational;

    sr_val_t* notif_val = NULL;
    perf_mgmt_notif_data_t* multi_notif_data = NULL;
    perf_mgmt_latest_notif_t* latest = NULL;
    int multi_item_cnt = 0;
    int latest_item_cnt = 0;
    int i, j, k;

    for (i = 0; i < USED_MEAS_GRP_CNT; i++) {
        for (j = 0; j < MAX_MEAS_OBJ_CNT; j++) {
            latest = &perf_mgmt_ctx->notif_ctx.latest_notif[i][j];
            if (latest->active) {
                latest_item_cnt += latest->data_cnt;
            }
        }
    }

    multi_item_cnt = perf_mgmt_ctx->notif_ctx.measurement_stats.data_cnt;
    if (latest_item_cnt == 0 || multi_item_cnt == 0) {
        return;
    }
    notif_val = (sr_val_t*)calloc(multi_item_cnt + latest_item_cnt, sizeof(sr_val_t));

    multi_notif_data = perf_mgmt_ctx->notif_ctx.measurement_stats.data_head;

    for (i = 0; i < multi_item_cnt; i++) {
        if (multi_notif_data == NULL) {
            OAM_LOG_ERROR("notif data count and multi_item_cnt are not equal");
        }
        if (multi_notif_data->values) {
            memcpy(&notif_val[i], multi_notif_data->values, sizeof(sr_val_t));
            multi_notif_data = multi_notif_data->next;
        }
    }

    k = i;
    for (i = 0; i < USED_MEAS_GRP_CNT; i++) {
        for (j = 0; j < MAX_MEAS_OBJ_CNT; j++) {
            latest = &perf_mgmt_ctx->notif_ctx.latest_notif[i][j];
            if (latest->active == 1) {
                for (size_t m = 0; m < latest->data_cnt; m++) {
                    memcpy(&notif_val[k + m], &latest->data[m], sizeof(sr_val_t));
                }
                k += latest->data_cnt;
            }
        }
    }
    sr_notif_send(session, "/o-ran-performance-management:measurement-result-stats", notif_val, multi_item_cnt + latest_item_cnt, 0, 0);

    free(notif_val);
    perf_mgmt_free_all_notif_data(perf_mgmt_ctx);
}

void notification_timer_handler(union sigval sv)
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = sv.sival_ptr;

    pthread_mutex_lock(&perf_mgmt_ctx->notif_ctx.notif_mutex);
    perf_mgmt_send_notif(perf_mgmt_ctx);
    pthread_mutex_unlock(&perf_mgmt_ctx->notif_ctx.notif_mutex);

    struct itimerspec its = { 0 };
    its.it_value.tv_sec = perf_mgmt_ctx->notif_ctx.interval;
    its.it_interval.tv_sec = perf_mgmt_ctx->notif_ctx.interval;
    timer_settime(perf_mgmt_ctx->notif_ctx.timerid, 0, &its, NULL);
}

static void* perf_mgmt_send_notif_thread(void* arg)
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = (oam_perf_mgmt_ctx_t*)arg;

    // Set up the timer
    struct sigevent sev = { 0 };
    sev.sigev_notify = SIGEV_THREAD;
    sev.sigev_notify_function = &notification_timer_handler;
    sev.sigev_notify_attributes = NULL;
    sev.sigev_value.sival_ptr = perf_mgmt_ctx;

    timer_t timerid;
    if (timer_create(CLOCK_REALTIME, &sev, &timerid) == -1) {
        OAM_LOG_ERROR("timer_create failed");
        return NULL;
    }

    // Set the timer interval
    struct itimerspec its = { 0 };
    its.it_value.tv_sec = perf_mgmt_ctx->notif_ctx.interval;
    its.it_interval.tv_sec = perf_mgmt_ctx->notif_ctx.interval;

    // Start the timer
    pthread_mutex_lock(&perf_mgmt_ctx->notif_ctx.notif_mutex);
    perf_mgmt_free_all_notif_data(perf_mgmt_ctx);
    pthread_mutex_unlock(&perf_mgmt_ctx->notif_ctx.notif_mutex);
    if (timer_settime(timerid, 0, &its, NULL) == -1) {
        OAM_LOG_ERROR("timer_settime failed");
        return NULL;
    }

    perf_mgmt_ctx->notif_ctx.timerid = timerid;

    while (perf_mgmt_ctx->notif_ctx.interval) {
        sleep(1);
    }
    timer_delete(timerid);
    perf_mgmt_ctx->notif_ctx.tid = 0;
    return NULL;
}

static void notification_interval_changed()
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();

    perf_mgmt_ctx->notif_ctx.interval = get_perf_mgmt_interval("notification");

    if (perf_mgmt_ctx->notif_ctx.interval == 0) {
        return;
    }

    if (check_any_active_meas_obj_exist() == 0) {
        OAM_LOG_WARN("notification interval is configured, but there is no active measurement object");
        perf_mgmt_ctx->notif_ctx.interval = 0;
        return;
    }

    if (!perf_mgmt_ctx->notif_ctx.tid) {
        pthread_create((pthread_t*)&perf_mgmt_ctx->notif_ctx.tid, NULL, perf_mgmt_send_notif_thread, (void*)perf_mgmt_ctx);
    }
}

void environmental_measurement_timer_handler(union sigval sv) // Environmental measurement timer handler (5 sec)
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = sv.sival_ptr; // Performance management context
    hal_config_t* hal_cfg = perf_mgmt_ctx->hal_cfg;
    epe_measurement_result_t* measurement_result;

    for (size_t i = 0; i < hal_cfg->sensor_num; i++) {
        if (hal_cfg->sensor_list[i].sensor_type != SENSOR_TYPE_TEMPERATURE) {
            continue;
        }

        float result;
        hal_cfg->sensor_list[i].read_sensor(hal_cfg, &hal_cfg->sensor_list[i], TEMPERATURE, &result);
        measurement_result = &perf_mgmt_ctx->epe_measurement_ctx.epe_result_list[i]; //set the measurement result for the sensor
        if (measurement_result->counter == 0) {
            measurement_result->monitor_result[TEMPERATURE][EPE_MEASUREMENT_REPORT_TYPE_MAXIMUM] = result;
            measurement_result->monitor_result[TEMPERATURE][EPE_MEASUREMENT_REPORT_TYPE_MINIMUM] = result;
            measurement_result->monitor_result[TEMPERATURE][EPE_MEASUREMENT_REPORT_TYPE_AVERAGE] = result;
            measurement_result->counter++;
            if (measurement_result->counter == (perf_mgmt_ctx->epe_measurement_ctx.interval / ENVIRONMENTAL_SAMPLE_INTERVAL)) {  // If the counter reaches the interval 
                measurement_result->counter = 0;
            }
            continue;
        }

        measurement_result->monitor_result[TEMPERATURE][EPE_MEASUREMENT_REPORT_TYPE_MAXIMUM]
            = MAX(result, measurement_result->monitor_result[TEMPERATURE][EPE_MEASUREMENT_REPORT_TYPE_MAXIMUM]);
        measurement_result->monitor_result[TEMPERATURE][EPE_MEASUREMENT_REPORT_TYPE_MINIMUM]
            = MIN(result, measurement_result->monitor_result[TEMPERATURE][EPE_MEASUREMENT_REPORT_TYPE_MINIMUM]);
        measurement_result->monitor_result[TEMPERATURE][EPE_MEASUREMENT_REPORT_TYPE_AVERAGE] = AVG_VALUE(result,
            measurement_result->monitor_result[TEMPERATURE][EPE_MEASUREMENT_REPORT_TYPE_AVERAGE] * measurement_result->counter, measurement_result->counter + 1);  // Calculate the average value
        measurement_result->counter++;
        if (measurement_result->counter == (perf_mgmt_ctx->epe_measurement_ctx.interval / ENVIRONMENTAL_SAMPLE_INTERVAL)) {
            measurement_result->counter = 0;
        }
    }

    struct itimerspec its = { 0 };

    its.it_value.tv_sec = ENVIRONMENTAL_SAMPLE_INTERVAL;  // Set the timer value to 5 seconds
    its.it_interval.tv_sec = ENVIRONMENTAL_SAMPLE_INTERVAL;   // Set the timer interval to 5 seconds
    timer_settime(perf_mgmt_ctx->epe_measurement_ctx.environmental_sample_timerid, 0, &its, NULL);
}

void energy_power_measurement_timer_handler(union sigval sv)  // Energy and power measurement timer handler (1 sec)
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = sv.sival_ptr;
    hal_config_t* hal_cfg = perf_mgmt_ctx->hal_cfg;
    epe_measurement_result_t* measurement_result;

    for (size_t i = 0; i < hal_cfg->sensor_num; i++) {
        if (hal_cfg->sensor_list[i].sensor_type != SENSOR_TYPE_POWER_RAIL) {
            continue;
        }
        // only read power and energy sensors
        float result;
        measurement_result = &perf_mgmt_ctx->epe_measurement_ctx.epe_result_list[i];
        for (size_t j = POWER; j < EPE_MEASUREMENT_OBJECT_NUM; j++) {   // EPE_MEASUREMENT_OBJECT_NUM is the number of measurement objects (DEFAULT = 4)
            hal_cfg->sensor_list[i].read_sensor(hal_cfg, &hal_cfg->sensor_list[i], j, &result);
            if (measurement_result->counter == 0) {
                measurement_result->monitor_result[j][EPE_MEASUREMENT_REPORT_TYPE_MAXIMUM] = result;
                measurement_result->monitor_result[j][EPE_MEASUREMENT_REPORT_TYPE_MINIMUM] = result;
                measurement_result->monitor_result[j][EPE_MEASUREMENT_REPORT_TYPE_AVERAGE] = result;
                continue;
            }

            measurement_result->monitor_result[j][EPE_MEASUREMENT_REPORT_TYPE_MAXIMUM] = MAX(result, measurement_result->monitor_result[j][EPE_MEASUREMENT_REPORT_TYPE_MAXIMUM]);
            measurement_result->monitor_result[j][EPE_MEASUREMENT_REPORT_TYPE_MINIMUM] = MIN(result, measurement_result->monitor_result[j][EPE_MEASUREMENT_REPORT_TYPE_MINIMUM]);
            measurement_result->monitor_result[j][EPE_MEASUREMENT_REPORT_TYPE_AVERAGE]
                = AVG_VALUE(result, measurement_result->monitor_result[j][EPE_MEASUREMENT_REPORT_TYPE_AVERAGE] * measurement_result->counter, measurement_result->counter + 1);
        }
        measurement_result->counter++;
        if (measurement_result->counter == (perf_mgmt_ctx->epe_measurement_ctx.interval / ENERGY_POWER_SAMPLE_INTERVAL)) {
            measurement_result->counter = 0;
        }
    }

    struct itimerspec its = { 0 };
    its.it_value.tv_sec = ENERGY_POWER_SAMPLE_INTERVAL;
    its.it_interval.tv_sec = ENERGY_POWER_SAMPLE_INTERVAL;
    timer_settime(perf_mgmt_ctx->epe_measurement_ctx.energy_power_sample_timerid, 0, &its, NULL);
}

typedef void (*timer_handler_function)(union sigval);

/**
 * @brief Create a POSIX timer and start it.
 *
 * @param[in,out] timerid The unique timer ID for every timer.
 * @param[in] timer_interval The interval of timer.
 * @param[in] timer_handler The handler of timer.
 */
void perf_mgmt_create_timer(timer_t* timerid, uint32_t timer_interval, timer_handler_function timer_handler)
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();

    // Set up the timer
    struct sigevent sev = { 0 };
    sev.sigev_notify = SIGEV_THREAD;
    sev.sigev_notify_function = timer_handler;
    sev.sigev_notify_attributes = NULL;
    sev.sigev_value.sival_ptr = perf_mgmt_ctx;

    if (timer_create(CLOCK_REALTIME, &sev, timerid) == -1) {
        OAM_LOG_ERROR("timer_create failed");
        return;
    }

    // Set the timer interval
    struct itimerspec its = { 0 };
    its.it_value.tv_sec = timer_interval;
    its.it_interval.tv_sec = timer_interval;

    // Start the timer
    if (timer_settime(*timerid, 0, &its, NULL) == -1) {
        OAM_LOG_ERROR("timer_settime failed");
        return;
    }
}

static void* perf_mgmt_epe_measurement_thread(void* arg)  // EPE measurement thread function
{
    sr_session_ctx_t* running_session = NULL;
    sr_session_ctx_t* operational_session = NULL;
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    int rc = sr_session_start(oam_netconf_ctx->netconf_ds_conn, SR_DS_RUNNING, &running_session);  // Get a session for the running datastore   
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("fail to getting running session %s", sr_strerror(rc));
        goto cleanup;
    }
    rc = sr_session_start(oam_netconf_ctx->netconf_ds_conn, SR_DS_OPERATIONAL, &operational_session);  // Get a session for the operational datastore
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("fail to getting operational session %s", sr_strerror(rc));
        goto cleanup;
    }

    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = (oam_perf_mgmt_ctx_t*)arg;

    perf_mgmt_create_timer(&perf_mgmt_ctx->epe_measurement_ctx.environmental_sample_timerid, ENVIRONMENTAL_SAMPLE_INTERVAL, environmental_measurement_timer_handler);  // Create a timer for environmental measurement
    perf_mgmt_create_timer(&perf_mgmt_ctx->epe_measurement_ctx.energy_power_sample_timerid, ENERGY_POWER_SAMPLE_INTERVAL, energy_power_measurement_timer_handler); // Create a timer for energy and power measurement

    perf_mgmt_measurement_result_t measurement_result;
    measurement_result.measurement_group = EPE_MEASUREMENT;
    measurement_result.measurement_interval = perf_mgmt_ctx->epe_measurement_ctx.interval;
    while (perf_mgmt_ctx->epe_measurement_ctx.interval) {
        sleep(perf_mgmt_ctx->epe_measurement_ctx.interval);
        measurement_result.epe_result = perf_mgmt_ctx->epe_measurement_ctx.epe_result_list;
        // fill latest measurement data
        perf_mgmt_fill_meas_result(running_session, operational_session, &measurement_result);

        // write data into file
        perf_mgmt_write_upload_file(running_session, &measurement_result);

        // write notification data
        perf_mgmt_write_notification(running_session, &measurement_result);
    }
    timer_delete(perf_mgmt_ctx->epe_measurement_ctx.environmental_sample_timerid);
    timer_delete(perf_mgmt_ctx->epe_measurement_ctx.energy_power_sample_timerid);
    perf_mgmt_ctx->epe_measurement_ctx.tid = 0;
cleanup:
    if (running_session) {
        sr_session_stop(running_session);
    }
    if (operational_session) {
        sr_session_stop(operational_session);
    }
    return NULL;
}

static void epe_measurement_interval_changed()
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();
    perf_mgmt_ctx->epe_measurement_ctx.interval = get_perf_mgmt_interval("epe-measurement");

    if (perf_mgmt_ctx->epe_measurement_ctx.interval == 0) {
        OAM_LOG_INFO("epe measurement interval is 0");
        return;
    }

    if (check_active_meas_obj_exist(EPE_MEASUREMENT) == 0) {  // If no active measurement object in EPE group
        perf_mgmt_ctx->epe_measurement_ctx.interval = 0;
        OAM_LOG_WARN("epe measurement interval is configured, but there is no related measurement object activated");
        return;
    }

    if (!perf_mgmt_ctx->epe_measurement_ctx.tid) {  // If the EPE measurement thread is not running
        /* clear last monitor result */
        for (size_t i = 0; i < perf_mgmt_ctx->epe_measurement_ctx.epe_result_list_num; i++) {  // Clear the last monitor result
            perf_mgmt_ctx->epe_measurement_ctx.epe_result_list[i].counter = 0;
            perf_mgmt_ctx->epe_measurement_ctx.epe_result_list[i].active_map = 0;
            memset(perf_mgmt_ctx->epe_measurement_ctx.epe_result_list[i].monitor_result, 0, sizeof(perf_mgmt_ctx->epe_measurement_ctx.epe_result_list[i].monitor_result));
        }

        pthread_create((pthread_t*)&perf_mgmt_ctx->epe_measurement_ctx.tid, NULL, perf_mgmt_epe_measurement_thread, (void*)perf_mgmt_ctx); // Create the EPE measurement thread
    }
}

/**
 * @brief Update the transceiver frequency bin table according to the latest measurement data.
 */
static void record_transceiver_freq_bin_table(perf_mgmt_transceiver_measurement_ctx_t* ctx)
{
    calc_transceiver_result_t calc_handler = NULL;
    uint16_t bin_count = 0;
    uint16_t bin_index = 0;
    double data = 0;

    for (size_t i = 0; i < TRANSCEIVER_MEASUREMENT_OBJECT_NUM; i++) {
        bin_count = ctx->result[i].bin_count;
        if (bin_count == 0) {
            continue;
        }
        calc_handler = get_transceiver_data_calc_handler(i);
        if (!calc_handler) {
            return;
        }
        switch (i) {
            case TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE:
                data = calc_handler(&ctx->diag_monitor_info, (uint16_t)ctx->rt_meas.temperature);
                break;
            case TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE:
                data = calc_handler(&ctx->diag_monitor_info, ctx->rt_meas.voltage);
                break;
            case TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT:
                data = calc_handler(&ctx->diag_monitor_info, ctx->rt_meas.tx_bias_current);
                break;
            case TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER:
                data = calc_handler(&ctx->diag_monitor_info, ctx->rt_meas.tx_power);
                break;
            case TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER:
                data = calc_handler(&ctx->diag_monitor_info, ctx->rt_meas.rx_power);
                break;
            case TRANSCEIVER_MEASUREMENT_OBJECT_NUM:
            default:
                break;
        }
        if (data < ctx->result[i].lower_bound) {
            ctx->result[i].freq_bin_table[0]++;
        } else if (data > ctx->result[i].upper_bound) {
            ctx->result[i].freq_bin_table[bin_count - 1]++;
        } else if (bin_count > 2) {
            bin_index = (uint16_t)floor((bin_count - 2) * (data - ctx->result[i].lower_bound) / (ctx->result[i].upper_bound - ctx->result[i].lower_bound) + 1);
            ctx->result[i].freq_bin_table[bin_index]++;
        }
    }
}

/**
 * @brief Update the transceiver record according to the latest measurement data.
 * Updated report info are MIN, MAX, FIRST, LATEST, frequency bin table.
 */
static void record_transceiver_measurement_result(perf_mgmt_transceiver_measurement_ctx_t* ctx)
{
    char* time_str = NULL;
    char local_time_str[LOCAL_TIME_STR_LEN];
    ly_time_time2str(time(NULL), NULL, &time_str);
    strncpy(local_time_str, time_str, sizeof(local_time_str));
    free(time_str);

    if (ctx->rt_meas.rx_power < ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER].min) {
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER].min = ctx->rt_meas.rx_power;
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER].min_time, local_time_str);
    } else if (ctx->rt_meas.rx_power > ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER].max) {
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER].max = ctx->rt_meas.rx_power;
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER].max_time, local_time_str);
    }
    if (ctx->rt_meas.tx_power < ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER].min) {
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER].min = ctx->rt_meas.tx_power;
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER].min_time, local_time_str);
    } else if (ctx->rt_meas.tx_power > ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER].max) {
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER].max = ctx->rt_meas.tx_power;
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER].max_time, local_time_str);
    }
    if (ctx->rt_meas.tx_bias_current < ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT].min) {
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT].min = ctx->rt_meas.tx_bias_current;
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT].min_time, local_time_str);
    } else if (ctx->rt_meas.tx_bias_current > ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT].max) {
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT].max = ctx->rt_meas.tx_bias_current;
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT].max_time, local_time_str);
    }
    if (ctx->rt_meas.voltage < ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE].min) {
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE].min = ctx->rt_meas.voltage;
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE].min_time, local_time_str);
    } else if (ctx->rt_meas.voltage > ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE].max) {
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE].max = ctx->rt_meas.voltage;
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE].max_time, local_time_str);
    }
    if (ctx->rt_meas.temperature < (int16_t)ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE].min) {
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE].min = (uint16_t)ctx->rt_meas.temperature;
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE].min_time, local_time_str);
    } else if (ctx->rt_meas.temperature > (int16_t)ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE].max) {
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE].max = (uint16_t)ctx->rt_meas.temperature;
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE].max_time, local_time_str);
    }

    if (!ctx->is_first_record_set) {
        ctx->is_first_record_set = true;
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER].first = ctx->rt_meas.rx_power;
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER].first = ctx->rt_meas.tx_power;
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT].first = ctx->rt_meas.tx_bias_current;
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE].first = ctx->rt_meas.voltage;
        ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE].first = (uint16_t)ctx->rt_meas.temperature;
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER].first_time, local_time_str);
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER].first_time, local_time_str);
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT].first_time, local_time_str);
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE].first_time, local_time_str);
        strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE].first_time, local_time_str);
    }

    ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER].latest = ctx->rt_meas.rx_power;
    ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER].latest = ctx->rt_meas.tx_power;
    ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT].latest = ctx->rt_meas.tx_bias_current;
    ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE].latest = ctx->rt_meas.voltage;
    ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE].latest = (uint16_t)ctx->rt_meas.temperature;
    strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_RX_POWER].latest_time, local_time_str);
    strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_POWER].latest_time, local_time_str);
    strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TX_BIAS_CURRENT].latest_time, local_time_str);
    strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_VOLTAGE].latest_time, local_time_str);
    strcpy(ctx->record[TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE].latest_time, local_time_str);

    record_transceiver_freq_bin_table(ctx);
    return;
}

/**
 * @brief Read the transceiver measurement data from SFP by I2C, and update the record in context.
 * All the report_info(min/max/first/latest) will be updated, no matter the measurement object is active or not.
 */
static bool get_transceiver_measurement_result(oam_perf_mgmt_ctx_t* perf_mgmt_ctx)
{
    perf_mgmt_transceiver_measurement_ctx_t* ctx = &perf_mgmt_ctx->transceiver_meas_ctx;

    uint8_t result_data[10] = { 0 };
    perf_mgmt_ctx->hal_cfg->retrieve_transceiver_realtime_monitor_data(result_data, 10);

    ctx->rt_meas.temperature = (int16_t)ntohs((uint16_t)result_data[0]);
    ctx->rt_meas.voltage = ntohs((uint16_t)result_data[2]);
    ctx->rt_meas.tx_bias_current = ntohs((uint16_t)result_data[4]);
    ctx->rt_meas.tx_power = ntohs((uint16_t)result_data[6]);
    ctx->rt_meas.rx_power = ntohs((uint16_t)result_data[8]);
    OAM_LOG_DEBUG("transceiver measurement raw data: temperature=%d, voltage=%d, tx_bias_current=%d, tx_power=%d, rx_power=%d", ctx->rt_meas.temperature, ctx->rt_meas.voltage,
        ctx->rt_meas.tx_bias_current, ctx->rt_meas.tx_power, ctx->rt_meas.rx_power);
    record_transceiver_measurement_result(ctx);

    return true;
}

/**
 * @brief Reset the transceiver measurement record.
 */
static void reset_transceiver_measurement_record(perf_mgmt_transceiver_measurement_ctx_t* ctx)
{
    ctx->is_first_record_set = false;
    for (size_t i = 0; i < TRANSCEIVER_MEASUREMENT_OBJECT_NUM; i++) {
        ctx->result[i].active = false;
        ctx->result[i].min_active = false;
        ctx->result[i].max_active = false;
        ctx->result[i].first_active = false;
        ctx->result[i].latest_active = false;

        ctx->record[i].min = UINT16_MAX;
        ctx->record[i].max = 0;
        ctx->record[i].first = 0;
        ctx->record[i].latest = 0;
        memset(ctx->record[i].min_time, 0, sizeof(ctx->record[i].min_time));
        memset(ctx->record[i].max_time, 0, sizeof(ctx->record[i].max_time));
        memset(ctx->record[i].first_time, 0, sizeof(ctx->record[i].first_time));
        memset(ctx->record[i].latest_time, 0, sizeof(ctx->record[i].latest_time));
        if (i == TRANSCEIVER_MEASUREMENT_OBJECT_TEMPERATURE) {
            ctx->record[i].min = (uint16_t)INT16_MAX;
            ctx->record[i].max = (uint16_t)INT16_MIN;
        }
    }
}

/**
 * @brief Reset the transceiver frequency bin table.
 * And update the bin count, lower bound and upper bound according to the configuration data in sysrepo.
 */
static void reset_transceiver_freq_bin_table(perf_mgmt_transceiver_measurement_ctx_t* ctx)
{
    const char* meas_obj_name = NULL;
    uint32_t meas_obj_id = 0;
    sr_val_t* values = NULL;
    size_t values_count = 0;
    sr_session_ctx_t* sess_for_run = get_oam_netconf_ctx()->netconf_ds_sess_running;

    char xpath[XPATH_LEN];
    snprintf(xpath, sizeof(xpath), "/o-ran-performance-management:performance-measurement-objects/transceiver-measurement-objects/bin-count");
    int ret = sr_get_items(sess_for_run, xpath, 0, 0, &values, &values_count);
    if (ret != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of transceiver-measurement-objects \"active\"");
        goto error;
    }
    for (size_t i = 0; i < values_count; i++) {
        meas_obj_name = extract_transceiver_meas_obj_name(values[i].xpath);
        meas_obj_id = (uint32_t)get_meas_obj_id(meas_obj_name, TRANSCEIVER_MEASUREMENT);

        if (ctx->result[meas_obj_id].bin_count != values[i].data.uint16_val) {
            if (ctx->result[meas_obj_id].freq_bin_table != NULL) {
                free(ctx->result[meas_obj_id].freq_bin_table);
            }
            ctx->result[meas_obj_id].bin_count = values[i].data.uint16_val;
            ctx->result[meas_obj_id].freq_bin_table = malloc(sizeof(uint32_t) * ctx->result[meas_obj_id].bin_count);
        }
        memset(ctx->result[meas_obj_id].freq_bin_table, 0, sizeof(uint32_t) * ctx->result[meas_obj_id].bin_count);
    }
    sr_free_values(values, values_count);

    snprintf(xpath, sizeof(xpath), "/o-ran-performance-management:performance-measurement-objects/transceiver-measurement-objects/lower-bound");
    ret = sr_get_items(sess_for_run, xpath, 0, 0, &values, &values_count);
    if (ret != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of transceiver-measurement-objects \"lower-bound\"");
        goto error;
    }
    for (size_t i = 0; i < values_count; i++) {
        meas_obj_name = extract_transceiver_meas_obj_name(values[i].xpath);
        meas_obj_id = (uint32_t)get_meas_obj_id(meas_obj_name, TRANSCEIVER_MEASUREMENT);
        ctx->result[meas_obj_id].lower_bound = values[i].data.decimal64_val;
    }
    sr_free_values(values, values_count);

    snprintf(xpath, sizeof(xpath), "/o-ran-performance-management:performance-measurement-objects/transceiver-measurement-objects/upper-bound");
    ret = sr_get_items(sess_for_run, xpath, 0, 0, &values, &values_count);
    if (ret != SR_ERR_OK) {
        OAM_LOG_ERROR("failed to get the values of transceiver-measurement-objects \"upper-bound\"");
        goto error;
    }
    for (size_t i = 0; i < values_count; i++) {
        meas_obj_name = extract_transceiver_meas_obj_name(values[i].xpath);
        meas_obj_id = (uint32_t)get_meas_obj_id(meas_obj_name, TRANSCEIVER_MEASUREMENT);
        ctx->result[meas_obj_id].upper_bound = values[i].data.decimal64_val;
    }

error:
    sr_free_values(values, values_count);
    return;
}

static void* transceiver_measurement_thread(void* arg)
{
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = (oam_perf_mgmt_ctx_t*)arg;
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* running_session = NULL;
    sr_session_ctx_t* operational_session = NULL;
    int rc = sr_session_start(oam_netconf_ctx->netconf_ds_conn, SR_DS_RUNNING, &running_session);
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("fail to getting running session: %s", sr_strerror(rc));
        goto cleanup;
    }
    rc = sr_session_start(oam_netconf_ctx->netconf_ds_conn, SR_DS_OPERATIONAL, &operational_session);
    if (rc != SR_ERR_OK) {
        OAM_LOG_ERROR("fail to getting operational session: %s", sr_strerror(rc));
        goto cleanup;
    }
    perf_mgmt_transceiver_measurement_ctx_t* ctx = &perf_mgmt_ctx->transceiver_meas_ctx;
    reset_transceiver_freq_bin_table(ctx);  // reset the frequency bin table before starting the measurement

    perf_mgmt_measurement_result_t result = { 0 };
    uint16_t seconds_elapsed = 0;
    while (ctx->interval) {
        if (seconds_elapsed < ctx->interval) {
            sleep(1);
            get_transceiver_measurement_result(perf_mgmt_ctx);  // read the transceiver measurement data from SFP by I2C
            seconds_elapsed++;
        } else {  // If the measurement interval has elapsed
            memset(&result, 0, sizeof(perf_mgmt_measurement_result_t));
            result.measurement_group = TRANSCEIVER_MEASUREMENT;
            result.measurement_interval = ctx->interval;

            // fill latest measurement data 
            perf_mgmt_fill_meas_result(running_session, operational_session, &result); // set coutnt on oper ds from i2c or smbus 

            // write notification data
            perf_mgmt_write_notification(running_session, &result);

            // write data into file
            perf_mgmt_write_upload_file(running_session, &result);
            seconds_elapsed = 0;
            reset_transceiver_measurement_record(ctx);  // reset the transceiver measurement record
            reset_transceiver_freq_bin_table(ctx); // reset the frequency bin table
        }
    }
    ctx->tid = 0;
cleanup:
    if (running_session) {
        sr_session_stop(running_session);
    }
    if (operational_session) {
        sr_session_stop(operational_session);
    }
    return NULL;
}

static void transceiver_measurement_interval_changed()
{
    int ret = 0;
    oam_perf_mgmt_ctx_t* ctx = get_oam_perf_mgmt_ctx();

    if (!ctx->transceiver_meas_ctx.diag_monitor_info.enabled) {
        return;
    }

    ctx->transceiver_meas_ctx.interval = get_perf_mgmt_interval("transceiver-measurement");

    ret = check_active_meas_obj_exist(TRANSCEIVER_MEASUREMENT);
    if (ret == 0) {
        ctx->transceiver_meas_ctx.interval = 0;
    }

    if (ctx->transceiver_meas_ctx.interval == 0) {
        return;
    }

    if (!ctx->transceiver_meas_ctx.tid) {
        pthread_create((pthread_t*)&ctx->transceiver_meas_ctx.tid, NULL, transceiver_measurement_thread, (void*)ctx);
    }

    return;
}

static int perf_mgmt_cfg_change_cb(sr_session_ctx_t* session, uint32_t sub_id, const char* module_name, const char* xpath, sr_event_t event, uint32_t request_id,
    void* private_data)
{
    int ret = SR_ERR_OK;
    uint64_t obj_change_map = 0;
    uint8_t itv_change_map = 0;

    if (event == SR_EV_CHANGE) {
        if (!is_hardware_admin_state_unlocked()) {
            sr_session_set_error_message(session, "Hardware admin state is not unlocked, please check /ietf-hardware:hardware/component/state/admin-state");
            return SR_ERR_UNAUTHORIZED;
        }
        ret = perf_mgmt_get_config_change(session, &obj_change_map, &itv_change_map);
        if (ret == 0) { // if there is no config change, return
            ret = perf_mgmt_send_msg_while_config_change(session, obj_change_map, itv_change_map);
        }
        return ret; // return error code if there is an error
    }

    if (event == SR_EV_DONE) {
        file_upload_interval_changed();
        notification_interval_changed();
        epe_measurement_interval_changed();
        transceiver_measurement_interval_changed();
    }

    return ret;
}

bool is_valid_serial_num(char* serial_num, int serial_num_len)
{
    for (int i = 0; i < serial_num_len && serial_num[i] != '\0'; i++) {
        if (serial_num[i] != 0xff) {
            return true;
        }
    }

    return false;
}

bool is_matched_serial_num(sr_session_ctx_t* session, char* xpath, char* serial_num)
{
    sr_val_t* values = NULL;
    size_t values_count = 0;
    sr_get_items(session, xpath, 0, 0, &values, &values_count);
    if (values_count == 0) {
        return false;
    } else if (values_count == 1 && !strcmp(values[0].data.string_val, serial_num)) {
        sr_free_values(values, values_count);
        return true;
    }

    sr_free_values(values, values_count);
    return false;
}

int fill_serial_num(hal_config_t* hal_cfg)
{
    int rc = SR_ERR_OK;
    /* set default radio name */
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();
    perf_mgmt_ctx->radio_unit_name = malloc(CONTENT_SIZE);
    perf_mgmt_ctx->radio_unit_name_with_serial_num = malloc(CONTENT_SIZE);
    snprintf(perf_mgmt_ctx->radio_unit_name, CONTENT_SIZE, "%s", "PicocomRU");
    snprintf(perf_mgmt_ctx->radio_unit_name_with_serial_num, CONTENT_SIZE, "%s", "PicocomRU");

    /* read serial num from eeprom */
    char serial_num[9] = { '\0' };
    get_serial_num(hal_cfg, serial_num, sizeof(serial_num) - 1); // i2c에서 읽은 serial num은 8바이트로, 마지막 '\0'을 위해 1바이트를 남겨둠 
    if (!is_valid_serial_num(serial_num, sizeof(serial_num))) {
        OAM_LOG_WARN("invalid serial number 0x%X 0x%X 0x%X 0x%X, 0x%X 0x%X 0x%X 0x%X, skip filling serial number", serial_num[0], serial_num[1], serial_num[2], serial_num[3],
            serial_num[4], serial_num[5], serial_num[6], serial_num[7]);
        return rc;  // 초기화 실패, serial num이 유효하지 않음
    }

    char xpath[XPATH_LEN];
    char root_xpath[] = "/ietf-hardware:hardware/component";
    snprintf(xpath, sizeof(xpath), "%s/class", root_xpath);
    sr_val_t* values = NULL;
    size_t values_count = 0;
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* sess_for_run = oam_netconf_ctx->netconf_ds_sess_running;
    sr_get_items(sess_for_run, xpath, 0, 0, &values, &values_count);

    char* data_xpath = NULL;
    char* radio_unit_name = NULL;
    sr_xpath_ctx_t state;
    for (size_t i = 0; i < values_count; i++) {
        if (!strcmp(values[i].data.identityref_val, "o-ran-hardware:O-RAN-RADIO")) {
            data_xpath = strdup(values[i].xpath);
            radio_unit_name = sr_xpath_key_value(data_xpath, "component", "name", &state); // 
            snprintf(xpath, sizeof(xpath), "%s[name='%s']/serial-num", root_xpath, radio_unit_name);
            break;
        }
    }

    sr_session_ctx_t* sess_for_oper = oam_netconf_ctx->netconf_ds_sess_operational;
    if (data_xpath && !is_matched_serial_num(sess_for_oper, xpath, serial_num)) {  // serial num이 oper 데이터베이스에 없으면
        rc = sr_set_item_str(sess_for_oper, xpath, serial_num, NULL, 0); // oper 데이터베이스에 serial num을 설정
        if (rc != SR_ERR_OK) {
            OAM_LOG_ERROR("failed to set serial number:%s", serial_num);
            return rc;
        }
        sr_apply_changes(sess_for_oper, 0);
        free(data_xpath);

        /* set radio name */
        snprintf(perf_mgmt_ctx->radio_unit_name, CONTENT_SIZE, "%s", radio_unit_name);
        snprintf(perf_mgmt_ctx->radio_unit_name_with_serial_num, CONTENT_SIZE, "%s_%s", radio_unit_name, serial_num);
    }

    sr_free_values(values, values_count);
    return rc;
}

static int perf_mgmt_init()
{
    int rc = 0;
    oam_perf_mgmt_ctx_t* perf_mgmt_ctx = get_oam_perf_mgmt_ctx();

    pthread_mutex_init(&perf_mgmt_ctx->file_mutex, NULL);
    pthread_mutex_init(&perf_mgmt_ctx->notif_ctx.notif_mutex, NULL);

    // allocate memory for sensors result.
    perf_mgmt_epe_measurement_ctx_t* epe_ctx = &perf_mgmt_ctx->epe_measurement_ctx;
    perf_mgmt_ctx->epe_measurement_ctx.epe_result_list_num = perf_mgmt_ctx->hal_cfg->sensor_num;
    epe_ctx->epe_result_list = malloc(epe_ctx->epe_result_list_num * sizeof(epe_measurement_result_t));
    for (size_t i = 0; i < epe_ctx->epe_result_list_num; i++) {
        epe_ctx->epe_result_list[i].sensor_name = perf_mgmt_ctx->hal_cfg->sensor_list[i].sensor_name;
        epe_ctx->epe_result_list[i].sensor_type = perf_mgmt_ctx->hal_cfg->sensor_list[i].sensor_type;
        if (perf_mgmt_ctx->hal_cfg->sensor_list[i].sensor_type == SENSOR_TYPE_TEMPERATURE) {
            epe_ctx->temperature_sensor_num++;
        } else if (perf_mgmt_ctx->hal_cfg->sensor_list[i].sensor_type == SENSOR_TYPE_POWER_RAIL) {
            epe_ctx->power_rail_sensor_num++;
        }
    }
    rc = fill_serial_num(perf_mgmt_ctx->hal_cfg);

    get_transceiver_meas_info(perf_mgmt_ctx->hal_cfg, &perf_mgmt_ctx->transceiver_meas_ctx);
    return rc;
}

int perf_mgmt_callback_register(hal_config_t* hal_config)
{
    int rc = SR_ERR_OK;
    oam_netconf_ctx_t* oam_netconf_ctx = get_oam_netconf_ctx();
    sr_session_ctx_t* session_for_running = oam_netconf_ctx->netconf_ds_sess_running;
    sr_subscription_ctx_t** subscription = &oam_netconf_ctx->netconf_ds_sub;

    get_oam_perf_mgmt_ctx()->hal_cfg = hal_config;

    rc = perf_mgmt_init();  // initialize performance management context
    if (rc != SR_ERR_OK) {
        return rc;
    }

    rc = sr_module_change_subscribe(session_for_running, "o-ran-performance-management", "/o-ran-performance-management:performance-measurement-objects/*", perf_mgmt_cfg_change_cb,
        NULL, 0, SR_SUBSCR_DEFAULT, subscription);
    if (rc != SR_ERR_OK) {
        return rc;
    }

    return rc;
}
