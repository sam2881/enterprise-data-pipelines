-- Validation naming migration
-- Rename vendor-specific GE tables while preserving legacy read/write names.

BEGIN;

DO $$
BEGIN
    IF to_regclass('public.ge_validation_result') IS NOT NULL
       AND to_regclass('public.validation_result') IS NULL THEN
        ALTER TABLE public.ge_validation_result RENAME TO validation_result;
    ELSIF to_regclass('public.ge_validation_result') IS NOT NULL
          AND to_regclass('public.validation_result') IS NOT NULL THEN
        RAISE EXCEPTION 'Both ge_validation_result and validation_result exist';
    END IF;

    IF to_regclass('public.ge_validation_summary') IS NOT NULL
       AND to_regclass('public.validation_summary') IS NULL THEN
        ALTER TABLE public.ge_validation_summary RENAME TO validation_summary;
    ELSIF to_regclass('public.ge_validation_summary') IS NOT NULL
          AND to_regclass('public.validation_summary') IS NOT NULL THEN
        RAISE EXCEPTION 'Both ge_validation_summary and validation_summary exist';
    END IF;
END $$;

ALTER INDEX IF EXISTS public.idx_ge_result_feed_run
    RENAME TO idx_validation_result_feed_run;
ALTER INDEX IF EXISTS public.idx_ge_result_created
    RENAME TO idx_validation_result_created;
ALTER INDEX IF EXISTS public.idx_ge_summary_feed_run
    RENAME TO idx_validation_summary_feed_run;

CREATE OR REPLACE VIEW public.ge_validation_result AS
SELECT * FROM public.validation_result;

CREATE OR REPLACE VIEW public.ge_validation_summary AS
SELECT * FROM public.validation_summary;

COMMIT;
