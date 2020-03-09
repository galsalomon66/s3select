#include "s3select.h"
#include "gtest/gtest.h"
using namespace s3selectEngine;

TEST(TestS3SElect, ParseQuery)  
{
    s3select s3select_syntax;
    const char* input_query = "select * from foobar";

    actionQ scm;

    s3select_syntax.load_schema(scm.schema_columns);

    ASSERT_EQ(s3select_syntax.parse_query(input_query), 0);
}

