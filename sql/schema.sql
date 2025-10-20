CREATE TABLE anomalies (
    country_code   TEXT           NOT NULL,
    start_date     DATE           NOT NULL,
    end_date       DATE           NOT NULL,
    score          DOUBLE PRECISION,
    residual       DOUBLE PRECISION,
    impact         DOUBLE PRECISION NOT NULL,
    quartile       SMALLINT       NOT NULL DEFAULT 0,   
    terms          TEXT[]         NOT NULL DEFAULT '{}'::text[],
    explanation_id INTEGER        REFERENCES explanations(id) ON DELETE SET NULL,

    PRIMARY KEY (country_code, start_date),

    CHECK (
        (score = 0   AND residual IS NOT NULL) OR
        (residual = 0 AND score    IS NOT NULL)
    ),
    CHECK (start_date <= end_date),
    CHECK (quartile BETWEEN 0 AND 3)  
);


/* helpful indexes for “contains” queries */
CREATE INDEX explanations_links_gin   ON explanations USING GIN (links);
CREATE INDEX explanations_tags_gin    ON explanations USING GIN (tags);
CREATE INDEX explanations_regions_gin ON explanations USING GIN (affected_regions);




CREATE TABLE anomalies (
    country_code   TEXT           NOT NULL,
    start_date     DATE           NOT NULL,
    end_date       DATE           NOT NULL,
    score          DOUBLE PRECISION,
    residual       DOUBLE PRECISION,
    impact         DOUBLE PRECISION NOT NULL,
    terms          TEXT[]         NOT NULL DEFAULT '{}'::text[],
    explanation_id INTEGER        REFERENCES explanations(id) ON DELETE SET NULL,

    PRIMARY KEY (country_code, start_date),

    CHECK (
        (score = 0   AND residual IS NOT NULL) OR
        (residual = 0 AND score    IS NOT NULL)
    ),
    CHECK (start_date <= end_date)
);




