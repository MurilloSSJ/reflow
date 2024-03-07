#!/bin/bash

DAG_DIRECTORY="/opt/reflow/dags"

get_all_dags() {
    local dir="$1"
    local dag_files=()

    for file in "$dir"/*; do
        if [[ -f "$file" && $file == *.py ]]; then
            if grep -qE '^[^#]*@DAG' "$file"; then
                dag_files+=("$file")
            fi
        elif [[ -d "$file" ]]; then
            # Chama recursivamente para os subdiretórios
            dag_files+=($(get_all_dags "$file"))
        fi
    done

    echo "${dag_files[@]}"
}

debugging_all_dags() {
    local all_dags=("$@")
    for dag_file in "${all_dags[@]}"; do
        echo "Validating $dag_file"
        error_output=$(python3 -m py_compile "$dag_file" 2>&1 >/dev/null)
        if [[ -n "$error_output" ]]; then
            echo "Error in $dag_file"
            data=$(jq -n --arg error_output "$error_output" --arg dag_file "$dag_file" '{ "error_log": $error_output, "absolute_path": $dag_file }')
            response=$(curl -s -X PATCH -H "Content-Type: application/json" -d "$data" http://reflow-api:3011/api/dags/set-error)
            echo "Response: $response"
        fi
    done
}

all_dags=($(get_all_dags "$DAG_DIRECTORY"))
debugging_all_dags "${all_dags[@]}"
echo "All DAG files:"
echo "${all_dags[@]}"