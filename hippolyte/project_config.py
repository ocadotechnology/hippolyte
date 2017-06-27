ACCOUNT_CONFIGS = {
    '123456789100': {
        'name': 'example-account',
        'emr_subnet': 'example-subnet-id',
        'log_bucket': 'hippolyte-eu-west-1-prod-backups',
        'backup_bucket': 'hippolyte-eu-west-1-prod-backups',
        'exclude_from_backup': [
            'example-table-*'
        ]
    }
}
