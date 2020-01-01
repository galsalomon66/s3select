#include "s3select.h"
#include <fstream>


class csv_object : public base_s3object
{

    private:

    bool is_object_open;
    FILE *m_fp;
    char buff[1024];
    base_statement *m_where_clause;
    list<base_statement *> m_projections;
    bool m_aggr_flow = false; //TODO once per query

    int getNextRow(char **tokens) //TODO add delimiter
    {//purpose: simple csv parser, not handling escape rules
               
        if (is_object_open == false)
        {
            if (m_obj_name.compare("stdin") != 0)
            {

                m_fp = fopen(m_obj_name.c_str(), "r");
            }
            else
            {
                m_fp = fdopen(0, "r");
            }

            if (!m_fp)
                throw base_s3select_exception("fail_to_open_csv");
            is_object_open = true;
        }

        char *st = fgets(buff, sizeof(buff), m_fp);
        if (!st)
        {
            if (m_obj_name.compare("stdin") != 0)
                fclose(m_fp);
            return -1;
        }
        
        char *p = buff;
        int i = 0;
        while (*p)
        {
            char *t = p;
            while (*p && (*(p) != ',')) //TODO delimiter
                p++;
            *p = 0;
            tokens[i++] = t;
            p++;
            if (*t==10) break;//trimming newline
        }
        tokens[i-1] = 0; //last token
        return i-1;
    }
    
public:
    csv_object(s3select *s3_query) : base_s3object(s3_query->get_from_clause().c_str(), s3_query->get_scratch_area())
    {
        is_object_open = false;
        m_fp = 0;
        m_projections = s3_query->get_projections_list();
        m_where_clause = s3_query->get_filter();

        //TODO projection is '*'
        if (m_where_clause)
            m_where_clause->traverse_and_apply(m_sa);

        for (auto p : m_projections)
            p->traverse_and_apply(m_sa);

        for (auto e : m_projections) //TODO for tests only, should be in semantic
        {
            base_statement *aggr = 0;

            if ((aggr = e->get_aggregate()) != 0)
            {
                if (aggr->is_nested_aggregate(aggr))
                {
                    throw base_s3select_exception("nested aggregation function is illegal i.e. sum(...sum ...)", base_s3select_exception::s3select_exp_en_t::FATAL);
                }

                m_aggr_flow = true;
            }
        }
        if (m_aggr_flow == true)
            for (auto e : m_projections)
            {
                base_statement *skip_expr = e->get_aggregate();

                if (e->is_binop_aggregate_and_column(skip_expr))
                {
                    throw base_s3select_exception("illegal expression. /select sum(c1) + c1 ..../ is not allow type of query", base_s3select_exception::s3select_exp_en_t::FATAL);
                }
            }

#if 0
        if (m_aggr_flow == true)
        {
            std::cout << "aggregated query" << std::endl;
        }
        else
        {
            std::cout << "not aggregated query" << std::endl;
        }
        //exit(0);
#endif
    }


public:
    int getMatchRow(std::list<string> &result) //TODO virtual ? getResult
    {
        int number_of_tokens = 0;
        char *row_tokens[128]; //TODO typedef for it     

        if (m_aggr_flow == true)
        {
            do
            {
                m_where_clause->traverse_and_release();

                number_of_tokens = getNextRow(row_tokens);
                if (number_of_tokens < 0) //end of stream
                {
                    for (auto i : m_projections)
                    {
                        i->set_last_call();
                        result.push_back((i->eval().to_string()));
                    }
                    return number_of_tokens;
                }

                m_sa->update((const char **)row_tokens, number_of_tokens);

                if (!m_where_clause || m_where_clause->eval().get_num() == true)
                    for (auto i : m_projections)
                                        i->eval();

            } while (1);
        }
        else
        {

            do
            {
                m_where_clause->traverse_and_release();

                number_of_tokens = getNextRow(row_tokens);
                if (number_of_tokens < 0)
                    return number_of_tokens;

                m_sa->update((const char **)row_tokens, number_of_tokens);
            } while (m_where_clause && m_where_clause->eval().get_num() == false);

            for (auto i : m_projections)
            {
                result.push_back(i->eval().to_string() );
                i->traverse_and_release();
            }
        }

        return number_of_tokens; //TODO wrong
    }
};

int cli_get_schema(const char *input_schema, actionQ &x)
{
    g_push_column.set_action_q(&x);

    rule<> column_name_rule = lexeme_d[(+alpha_p >> *digit_p)];

    //TODO an issue to resolve with trailing space
    parse_info<> info = parse(input_schema, ((column_name_rule)[BOOST_BIND_ACTION(push_column)] >> *(',' >> (column_name_rule)[BOOST_BIND_ACTION(push_column)])), space_p);

    if (!info.full)
    {
        std::cout << "failure in schema description " << input_schema << std::endl;
        return -1;
    }

    return 0;
}

#if 0
int test_value(int argc,char **argv)
{
    {
    value a(10),b(11);

    printf("a>b :: %d , a<b :: %d , a>=b :: %d  , a<=b :: %d , a==b :: %d , a!=b :: %d    { %d,%d } \n", a>b , a<b , a>=b , a<=b , a==b , a!=b , a.__val.num , b.__val.num);
    }

    {
    value a(10),b(11),c((a==b));
    printf("c = %d    %d\n",c.__val.num , value(a<b).__val.num );
    }
    
    {
    value a(10),b(10.0);

    printf("a>b :: %d , a<b :: %d , a>=b :: %d  , a<=b :: %d , a==b :: %d , a!=b :: %d    { %d,%f } \n", a>b , a<b , a>=b , a<=b , a==b , a!=b , a.__val.num , b.__val.dbl);
    }

    {
    value a(strdup("abcd")),b(strdup("abcd"));

    printf("a>b :: %d , a<b :: %d , a>=b :: %d  , a<=b :: %d , a==b :: %d , a!=b :: %d    { %s , %s } \n", a>b , a<b , a>=b , a<=b , a==b , a!=b , a.__val.str , b.__val.str);
    }

    {
        value a(2), b(10.1);
        std::cout << "a+b  = " << (a + b).__val.dbl << std::endl;
    }

    {
        value a(2), b(10.1);
        std::cout << "a-b  = " << (a - b).__val.dbl << std::endl;
    }

    {
        value a(2), b(10.1);
        std::cout << "a*b  = " << (a * b).__val.dbl << std::endl;
    }

    {
        value a(2.0), b(10.0);
        std::cout << "a/b  = " << (a / b).__val.dbl << std::endl;
        std::cout << "a/b  = " << (a / b).__val.num << std::endl;
    }

    {
        value a(2), b(10);
        std::cout << "a ^ b  = " << (a ^ b).__val.dbl << std::endl;
    }
    {
        value a(2), b(10);
        std::cout << "a ^ b  = " << (a ^ b).__val.num << std::endl;
    }
}
#endif

int main(int argc,char **argv)
{
//purpose: demostrate the s3select functionalities 
    s3select s3select_syntax;

    char *input_schema =0, *input_query =0;

    for(int i=0;i<argc;i++)
    {
        if (!strcmp(argv[i] ,"-s")) input_schema = argv[i+1];
        if (!strcmp(argv[i] ,"-q")) input_query = argv[i+1];
    }

    actionQ scm;
    if (input_schema && cli_get_schema(input_schema, scm) < 0) {
        std::cout << "input schema is wrong" << std::endl;
        return -1;
    }

    if(!input_query){
        std::cout << "type -q 'select ... from ...  '" << std::endl;
        return -1;
    }

    s3select_syntax.load_schema(scm.schema_columns);

	parse_info<> info = parse( input_query , s3select_syntax, space_p);//TODO object
    auto x = info.stop;

    if(!info.full){std::cout << "failure -->" << x << "<---" << std::endl;return -1;}
    
    //std::cout << s3select_syntax.get_filter()->print(40) << std::endl;

    //return -1;

    try
    {
        csv_object my_input(&s3select_syntax);

        do
        {
            std::list<string> result;
            int num = my_input.getMatchRow(result);

            for( auto s : result)
                std::cout << s << "," ;
            std::cout << std::endl;

            if (num<0)
                break;


        } while (1);
    }
    catch (base_s3select_exception e)
    {   
        std::cout << e.what() << std::endl;
        return -1;
    }

    
}
