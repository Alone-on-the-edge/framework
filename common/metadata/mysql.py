class MysqlMetadataProvider(MetadataProvider):

    def __init__(self, meta_conf: MetadataConfig, job_ctx: JobContext):
        super().__init__(meta_conf, job_ctx)