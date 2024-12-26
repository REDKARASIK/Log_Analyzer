#include <iostream>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <ctime>

//----------------------------------------------------------------------
// Constants
const size_t kBufferSize = 8192;
const int kDozens = 10;
const int kHundreds = 100;
const int kThousands = 1000;
const int KYearOfStartCountTimestamp = 1970;
const int kDaysInYear = 365;
const int kHoursInDay = 24;
const int kMinutesInHour = 60;
const int kSecondsInMinute = 60;
const int kZeroIndexInAscii = 48;
const int kLeapYearDeterminant = 4;
const int kCurrentDay = 1;
const int kIndexForRemoteAddr = 1;
const int kIndexesForLocalTime[2] = {4, 5};
const int kIndexForRequest = 6;
const int kModCompareForLeapYear = 2;
const int kDaysByMoth[11] = {31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

//----------------------------------------------------------------------
// Structs and Enums
struct TerminalArgs {
    const char* output = nullptr;
    const char* input = nullptr;
    bool print = false;
    int stats = 10;
    int window = 0;
    int from_time = INT_MIN;
    int to_time = INT_MAX;
};

struct LogsParts {
    std::string remote_addr;
    std::string local_time;
    std::string request;
    std::string status;
    std::string bytes_send;
    int timestamp = 0;

};

struct WindowParams {
    int max_count_of_request = 0;
    int timestamp_start = 0;
    int timestamp_end = 0;
    std::vector<int> timestamp_window;
};

enum FinishCode {
    kSuccess,
    kError
};

//----------------------------------------------------------------------
// Help Functions
bool CheckStringIsNumber(char* number) {
    while (*number != '\0') {
        if (!isdigit(*number)) {
            return false;
        }
        ++number;
    }
    return true;
}

int SearchEqualInChar(char*& arg) {
    int equal_index = 0;
    while (*arg != '\0') {
        if (*arg == '=') {
            return equal_index;
        }
        ++arg;
        ++equal_index;
    }
    return equal_index;
}

int ConvertDateToTimestamp(std::string date) {
    int day = (date[1] - kZeroIndexInAscii) * kDozens + (date[2] - kZeroIndexInAscii);
    char month[] = {date[4], date[5], date[6], '\0'};
    int year = (date[8] - kZeroIndexInAscii) * kThousands + (date[9] - kZeroIndexInAscii) * kHundreds + (date[10] - kZeroIndexInAscii) * kDozens + (date[11] - kZeroIndexInAscii) - KYearOfStartCountTimestamp;
    int hours = (date[13] - kZeroIndexInAscii) * kDozens + (date[14] - kZeroIndexInAscii);
    int minutes = (date[16] - kZeroIndexInAscii) * kDozens + (date[17] - kZeroIndexInAscii);
    int seconds = (date[19] - kZeroIndexInAscii) * kDozens + (date[20] - kZeroIndexInAscii);
    int greenwich_mean_time = (((date[23] - kZeroIndexInAscii) * kDozens + (date[24] - kZeroIndexInAscii)) * kMinutesInHour + (date[25] - kZeroIndexInAscii) * kMinutesInHour + (date[26] - kZeroIndexInAscii)) * kSecondsInMinute;
    if (date[22] == '-') {
        greenwich_mean_time *= -1;
    }
    int timestamp = (((year * kDaysInYear + (year + kModCompareForLeapYear) / kLeapYearDeterminant + day - kCurrentDay) * kHoursInDay + hours) * kMinutesInHour + minutes) * kSecondsInMinute + seconds - greenwich_mean_time;
    if (strcmp(month, "Jan") == 0 && year % kLeapYearDeterminant == kModCompareForLeapYear) {
        timestamp -= kHoursInDay * kMinutesInHour * kSecondsInMinute;
    } else if (strcmp(month, "Feb") == 0) {
        if (year % kLeapYearDeterminant == kModCompareForLeapYear) {
            timestamp -= kHoursInDay * kMinutesInHour * kSecondsInMinute;
        }
        timestamp += kDaysByMoth[0] * kHoursInDay * kMinutesInHour * kSecondsInMinute;
    } else if (strcmp(month, "Mar") == 0) {
        timestamp += kDaysByMoth[1] * kHoursInDay * kMinutesInHour * kSecondsInMinute;
    } else if (strcmp(month, "Apr") == 0) {
        timestamp += kDaysByMoth[2] * kHoursInDay * kMinutesInHour * kSecondsInMinute;
    } else if (strcmp(month, "May") == 0) {
        timestamp += kDaysByMoth[3] * kHoursInDay * kMinutesInHour * kSecondsInMinute;
    } else if (strcmp(month, "Jun") == 0) {
        timestamp += kDaysByMoth[4] * kHoursInDay * kMinutesInHour * kSecondsInMinute;
    } else if (strcmp(month, "Jul") == 0) {
        timestamp += kDaysByMoth[5] * kHoursInDay * kMinutesInHour * kSecondsInMinute;
    } else if (strcmp(month, "Aug") == 0) {
        timestamp += kDaysByMoth[6] * kHoursInDay * kMinutesInHour * kSecondsInMinute;
    } else if (strcmp(month, "Sep") == 0) {
        timestamp += kDaysByMoth[7] * kHoursInDay * kMinutesInHour * kSecondsInMinute;
    } else if (strcmp(month, "Oct") == 0) {
        timestamp += kDaysByMoth[8] * kHoursInDay * kMinutesInHour * kSecondsInMinute;
    } else if (strcmp(month, "Nov") == 0) {
        timestamp += kDaysByMoth[9] * kHoursInDay * kMinutesInHour * kSecondsInMinute;
    } else {
        timestamp += kDaysByMoth[10] * kHoursInDay * kMinutesInHour * kSecondsInMinute;
    }
    return timestamp;
}

int GetSizeOfSublineByLine(const std::string& line) {
    std::string temp_subline;
    std::stringstream sublines(line);
    int size_subline = 0;
    while (sublines >> temp_subline) {
        ++size_subline;
    }
    return size_subline;
}

//----------------------------------------------------------------------
// Errors and Check files functions
int RiseErrorIncorrectNumber() {
    std::cerr << "Incorrect value for <int> type argument" << '\n';
    return kError;
}

int CheckIsIndexIsOutOfRangeAndRiseError(int i, int argc, char* argv) {
    if (i == argc - 1){
        std::cerr << "No value for '" << argv << "'\n";
        return kError;
    }
    return kSuccess;
}

int RiseErrorIncorrectArgument() {
    std::cerr << "Getting incorrect arguments" << '\n';
    return kError;
}

int CheckFileIsNotSpecified(const char*& filename_input, const char*& filename_output) {
    if (filename_input == nullptr && filename_output == nullptr) {
        std::cerr << "Files is not specified" << '\n';
        return kError;
    } else if (filename_output == nullptr) {
        std::cerr << "Output file is not specified" << '\n';
        return kError;
    } else if (filename_input == nullptr) {
        std::cerr << "Input file is not specified" << '\n';
        return kError;
    }
    return kSuccess;
}

int CheckFileIsNotOpen(FILE*& file_input, FILE*& file_output) {
    if (!file_input && !file_output) {
        std::cerr << "Cannot open files by path, please check all paths of your files." << '\n';
        return kError;
    } else if (!file_input) {
        std::cerr << "Cannot open input file by path, please check path of your input file." << '\n';
        return kError;
    } else if (!file_output) {
        std::cerr << "Cannot open output file by path, please check path of your output file." << '\n';
        return kError;
    }
    return kSuccess;
}

//----------------------------------------------------------------------
// Read args from Command Line
int ReadLongArgs(char* arg, TerminalArgs* args) {
    char *arg_suffix;
    int equal_index = SearchEqualInChar(arg);
    arg_suffix = arg + 1;
    arg = arg - equal_index;
    arg[equal_index] = '\0';
    if (*arg_suffix == '\0'){
        return RiseErrorIncorrectArgument();
    }
    if (strcmp(arg, "--output") == 0 || strcmp(arg, "-o") == 0) {
        args->output = arg_suffix;
    } else if (strcmp(arg, "--stats") == 0 || strcmp(arg, "-s") == 0) {
        if (!CheckStringIsNumber(arg_suffix)) {
            return RiseErrorIncorrectNumber();
        }
        args->stats = std::stoi(arg_suffix);
    } else if (strcmp(arg, "--window") == 0 || strcmp(arg, "-w") == 0) {
        if (!CheckStringIsNumber(arg_suffix)) {
            return RiseErrorIncorrectNumber();
        }
        args->window = std::stoi(arg_suffix);
    } else if (strcmp(arg, "--from") == 0 || strcmp(arg, "-f") == 0) {
        if (!CheckStringIsNumber(arg_suffix)) {
            return RiseErrorIncorrectNumber();
        }
        args->from_time = std::stoi(arg_suffix);
    } else if (strcmp(arg, "--to") == 0 || strcmp(arg, "-e") == 0) {
        if (!CheckStringIsNumber(arg_suffix)) {
            return RiseErrorIncorrectNumber();
        }
        args->to_time = std::stoi(arg_suffix);
    } else {
        if (args->input != nullptr){
            return RiseErrorIncorrectArgument();
        }
        args->input = arg;
    }
    return kSuccess;
}

int ReadArgs(int argc, char** argv, TerminalArgs* args) {
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--print") == 0) {
            args->print = true;
        } else if (strcmp(argv[i], "-o") == 0 || strcmp(argv[i], "--output") == 0) {
            if (CheckIsIndexIsOutOfRangeAndRiseError(i, argc, argv[i]) == kError){
                return kError;
            }
            args->output = argv[++i];
        } else if (strcmp(argv[i], "-s") == 0 || strcmp(argv[i], "--stats") == 0) {
            if (CheckIsIndexIsOutOfRangeAndRiseError(i, argc, argv[i]) == kError){
                return kError;
            }
            if (!CheckStringIsNumber(argv[i + 1])) {
                return RiseErrorIncorrectNumber();
            }
            args->stats = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "-w") == 0 || strcmp(argv[i], "--window") == 0) {
            if (CheckIsIndexIsOutOfRangeAndRiseError(i, argc, argv[i]) == kError){
                return kError;
            }
            if (!CheckStringIsNumber(argv[i + 1])) {
                return RiseErrorIncorrectNumber();
            }
            args->window = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "-f") == 0 || strcmp(argv[i], "--from") == 0) {
            if (CheckIsIndexIsOutOfRangeAndRiseError(i, argc, argv[i]) == kError){
                return kError;
            }
            if (!CheckStringIsNumber(argv[i + 1])) {
                return RiseErrorIncorrectNumber();
            }
            args->from_time = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "-e") == 0 || strcmp(argv[i], "--to") == 0) {
            if (CheckIsIndexIsOutOfRangeAndRiseError(i, argc, argv[i]) == kError){
                return kError;
            }
            if (!CheckStringIsNumber(argv[i + 1])) {
                return RiseErrorIncorrectNumber();
            }
            args->to_time = std::stoi(argv[++i]);
        } else if (ReadLongArgs(argv[i], args) == kError) {
            return kError;
        }
    }
    return kSuccess;
}

//----------------------------------------------------------------------
// Working with Files
void ClosingFiles(FILE*& file_input, FILE*& file_output) {
    fclose(file_input);
    fclose(file_output);
}

bool IsIncorrectLog(LogsParts* log_parts) {
    return (!(log_parts->local_time.size() == 28 && log_parts->local_time[0] == '[' &&
     log_parts->local_time[log_parts->local_time.size() - 1] == ']')) && (!(log_parts->request.size() >= 2 && 
     log_parts->request[0] == '"' && log_parts->request[log_parts->request.size() - 1] == '"'));
}

int SplitLog(LogsParts* log_parts, std::string line) {
    std::stringstream sublines(line);
    const int size_subline = GetSizeOfSublineByLine(line);
    int index_subline = 1;
    std::string temp_string;
    while (sublines >> temp_string) {
        if (index_subline == kIndexForRemoteAddr) {
            log_parts->remote_addr = temp_string;
        } else if (index_subline == kIndexesForLocalTime[0]) {
            log_parts->local_time += temp_string;
        } else if (index_subline == kIndexesForLocalTime[1]) {
            log_parts->local_time += " " + temp_string;
        } else if (index_subline == size_subline - 1) {
            log_parts->status = temp_string;
        } else if (index_subline == size_subline) {
            log_parts->bytes_send = temp_string;
        } else if (index_subline >= kIndexForRequest) {
            if (log_parts->request == "") {
                log_parts->request += temp_string;
            } else {
                log_parts->request += " " + temp_string;
            }
        }
        ++index_subline;
    }
    if (IsIncorrectLog(log_parts)){
        return kError;
    }
    log_parts->timestamp = ConvertDateToTimestamp(log_parts->local_time);
    return kSuccess;
}

bool CheckLogForArgs(LogsParts* log, TerminalArgs* args) {
    bool check_exit_status_code = false;
    bool check_in_time_interval = false;
    if (log->status[0] == '5' && log->status.size() == 3 && (log->status.find_first_not_of("0123456789") == log->status.npos)) {
        check_exit_status_code = true;
    }
    if (args->from_time <= log->timestamp && log->timestamp <= args->to_time) {
        check_in_time_interval = true;
    }
    return check_exit_status_code && check_in_time_interval;
}

void AddLogToStats(LogsParts* log, std::unordered_map<std::string, int>& stats_for_requests) {
    if (stats_for_requests.count(log->request) > 0) {
        ++stats_for_requests[log->request];
    } else {
        stats_for_requests[log->request] = 1;
    }
}

void PrintSortStats(TerminalArgs* args, std::unordered_map<std::string, int>& stats_for_requests) {
    std::vector<std::pair<int, std::string>> reverse_stats_for_requests;
    for (const std::pair<std::string, int>& item: stats_for_requests) {
        reverse_stats_for_requests.push_back(std::make_pair(item.second, item.first));
    }
    std::sort(reverse_stats_for_requests.rbegin(), reverse_stats_for_requests.rend());
    int count_stats = 0;
    std::cout << "\nRequest Statistic:\n\n";
    for (const std::pair<int, std::string>& item: reverse_stats_for_requests) {
        if (count_stats == args->stats) {
            break;
        }
        std::cout << count_stats + 1 << ". REQUEST: " << item.second << "\tFREQUENCY: " << item.first << '\n';
        ++count_stats;
    }
    std::cout << "\nTOTAL STATS: " << count_stats << "\n---------------------------\n";
}

void GettingWindow(LogsParts* log, WindowParams* window_params, TerminalArgs* args) {
    if (window_params->timestamp_window.size() == 0) {
        window_params->timestamp_window.push_back(log->timestamp);
        window_params->timestamp_start = log->timestamp;
        return;
    }
    if ((log->timestamp - window_params->timestamp_window[0]) <= args->window) {
        window_params->timestamp_window.push_back(log->timestamp);
    } else {
        if (window_params->max_count_of_request < window_params->timestamp_window.size()) {
            window_params->max_count_of_request = window_params->timestamp_window.size();
            window_params->timestamp_start = window_params->timestamp_window[0];
            window_params->timestamp_end = window_params->timestamp_window[window_params->timestamp_window.size() - 1];
        }
        while ((log->timestamp - window_params->timestamp_window[0]) > args->window) {
            window_params->timestamp_window.erase(window_params->timestamp_window.begin());
            if (window_params->timestamp_window.size() == 0) {
                break;
            }
        }
        window_params->timestamp_window.push_back(log->timestamp);
        }
}

void PrintWindow(WindowParams* window) {
    if (window->max_count_of_request < window->timestamp_window.size()) {
            window->max_count_of_request = window->timestamp_window.size();
            window->timestamp_start = window->timestamp_window[0];
            window->timestamp_end = window->timestamp_window[window->timestamp_window.size() - 1];
    }
    std::cout << "\nWindow with the biggest count of requests:\n\n";
    if (window->max_count_of_request > 0){
        time_t time_start = window->timestamp_start;
        time_t time_end = window->timestamp_end;
        std::cout << "START: " << ctime(&time_start);
        std::cout << "END: " << ctime(&time_end) << '\n';
    }
    std::cout << "TOTAL REQUESTS IN WINDOW: "<< window->max_count_of_request <<"\n---------------------------\n";
}

int AnalyseLog(const std::string& line, TerminalArgs* args, std::unordered_map<std::string, int>& stats_for_requests, WindowParams* window_params) {
    LogsParts log_parts;
    if (SplitLog(&log_parts, line) == kError) {
        return kError;
    }
    if (args->window > 0 && (args->from_time <= log_parts.timestamp && log_parts.timestamp <= args->to_time)) {
        GettingWindow(&log_parts, window_params, args);
    }
    if (CheckLogForArgs(&log_parts, args)) {
        if (args->stats > 0) {
            AddLogToStats(&log_parts, stats_for_requests);
        }
        return kSuccess;
    }
    return kError;
}

void PrintInStdout(char* line, TerminalArgs* args) {
    if (args->print) {
        std::cout << line;
    }
}

int StartToWorkWithLogs(TerminalArgs* args) {
    if (CheckFileIsNotSpecified(args->input, args->output) == kError){
        return kError;
    }
    FILE* file_input = fopen(args->input, "rb");
    FILE* file_output = fopen(args->output, "wb");
    if (CheckFileIsNotOpen(file_input, file_output) == kError){
        return kError;
    }
    WindowParams window_params;
    std::unordered_map<std::string, int> stats_for_requests;
    char buffer[kBufferSize];
    int amount_of_logs_with_5xx_status_code = 0;
    while (std::fgets(buffer, kBufferSize, file_input)) {
        if (AnalyseLog(buffer, args, stats_for_requests, &window_params) == kSuccess) {
            fputs(buffer, file_output);
            ++amount_of_logs_with_5xx_status_code;
            PrintInStdout(buffer, args);
        }
    }
    std::cout << "\nTOTAL LOGS WITH STATUS 5XX: " << amount_of_logs_with_5xx_status_code << "\n---------------------------\n";
    if (args->stats > 0) {
        PrintSortStats(args, stats_for_requests);
    }
    if (args->window > 0) {
        PrintWindow(&window_params);
    }
    ClosingFiles(file_input, file_output);
    return kSuccess;
}

//----------------------------------------------------------------------
// Main
int main(int argc, char** argv) {
    TerminalArgs args;
    if (ReadArgs(argc, argv, &args) == kError){
        return kError;
    }
    if (StartToWorkWithLogs(&args) == kError){
        return kError;
    }
    return kSuccess;
}
