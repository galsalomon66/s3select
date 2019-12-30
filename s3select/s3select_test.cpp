#include "s3select.h"
#include <fstream>


class csv_object : public base_s3object
{
public:

    csv_object(s3select * s3_query) : base_s3object(s3_query->get_from_clause().c_str(), s3_query->get_scratch_area())
    {
        is_object_open = false;
        m_fp = 0;
        m_projections = s3_query->get_projections_list();
        m_where_clause = s3_query->get_filter();

        //TODO projection is '*'
        if (m_where_clause) m_where_clause->traverse_and_apply(m_sa);

        for(auto p : m_projections )
            p->traverse_and_apply(m_sa); 
    }

private:
    bool is_object_open;
    FILE *m_fp;
    char buff[1024];
    base_statement* m_where_clause;
    list<base_statement *> m_projections;


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
    int getMatchRow(std::list<string> & result) //TODO virtual ?
    {
        int number_of_tokens = 0;
        result;
        
        char * row_tokens[128];//TODO typedef for it
        do
        {
            number_of_tokens = getNextRow(row_tokens);
            if (number_of_tokens < 0)
                return number_of_tokens; 

            m_sa->update((const char **)row_tokens, number_of_tokens);
        } while (m_where_clause && m_where_clause->eval() == false);
        
        int col_no = 0;
        for( auto i : m_projections){
            result.push_back( std::to_string( i->eval() ) );
        }

        return number_of_tokens;
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
        std::cout << "failure" << std::endl;
        return -1;
    }

    return 0;
}


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


    try
    {
        csv_object my_input(&s3select_syntax);

        do
        {
            std::list<string> result;
            int num = my_input.getMatchRow(result);
            if (num<0)
                break;

            for( auto s : result)
                std::cout << s << "," ;

            std::cout << std::endl;

        } while (1);
    }
    catch (base_s3select_exception e)
    {   
        std::cout << e.what() << std::endl;
        return -1;
    }
}
