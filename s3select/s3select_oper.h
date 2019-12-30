#include <string>
#include <iostream>
#include <list>
#include <map>
#include <string.h>

using namespace std;
class base_s3select_exception { 
    public:
        const char * _msg;
        base_s3select_exception(const char * n){ _msg = n;}

        virtual const char* what(){return _msg; }
};


class scratch_area
{

private:
    const char *m_columns[128];
    int m_upper_bound;

    list<pair<std::string,int >> m_column_name_pos;

public:

    void set_column_pos(const char *n, int pos)//TODO use std::string
    {
        m_column_name_pos.push_back( pair<const char*,int>(n,pos));
    }

    void update(const char **tokens, int num_of_tokens)
    {
        if (num_of_tokens > (sizeof(m_columns)/sizeof(char*))){
            throw base_s3select_exception("too_many_tokens");
        };

        m_upper_bound = num_of_tokens;

        for (int i = 0; i < num_of_tokens; i++)
        {
            m_columns[i] = tokens[i];//TODO bound check
        }
    }

    int get_column_pos(const char *n)
    {//done only upon building the AST , not on "runtime"

        list<pair<std::string,int >>::iterator iter;
        
        for( auto iter : m_column_name_pos)
        {
            if (!strcmp(iter.first.c_str(),n)) return iter.second;
        }

        throw base_s3select_exception("column_name_not_in_schema");
    }

    int get_column_value(int column_pos) //retrive value per column position (should return always string? )
    {
        if ( (column_pos >= m_upper_bound) || column_pos < 0) 
            throw base_s3select_exception("column_position_is_wrong"); 

        return atoi(m_columns[column_pos]); //TODO temporary; should use class value { ..... };
    }
};

class base_statement {

    protected:

    scratch_area *m_scratch;

	public:
        base_statement():m_scratch(0){}
		virtual int eval() =0;
		virtual void print(int ident) =0;
        virtual bool semantic() =0;//done once , post syntax , traverse all nodes and validate semantics. 
        virtual void traverse_and_apply(scratch_area *) =0;//done once , post parsing , it "spread" the scratch-area to all nodes(variables)
};

class variable : public base_statement
{

private:
    int number;
    std::string _name;
    int column_pos;

public:

    typedef enum
    {
        VAR,
        NUM,
        POS
    } var_t; 
    var_t m_var_type;
    

    int num() { return number; }         //TODO validate type
    std::string name() { return _name; } //TODO validate name

    variable(int i) : number(i), m_var_type(NUM), _name("#"), column_pos(-1){}
    variable(const char *n) : _name(n), m_var_type(VAR), number(0), column_pos(-1){}
    
    variable(const char *n ,  var_t tp) : _name(n), m_var_type(tp), number(0)
    {
            int pos = atoi( n+1 ); //TODO >0 < (schema definition , semantic analysis)
            column_pos = pos -1;// _1 is the first column ( zero position )
    }

    virtual int eval()
    {
        if (m_var_type == NUM)
            return number;
        else if (column_pos == -1)
            column_pos = m_scratch->get_column_pos(_name.c_str());//done once , for the first time

        return m_scratch->get_column_value(column_pos);
    }

    virtual void traverse_and_apply(scratch_area *sa)
    {
        m_scratch = sa;
    }

    virtual void print(int ident)
    {
        std::cout << "var:" << number;
    }

    virtual bool semantic()
    {return false;
    }
};

class arithmetic_operand : public base_statement {

	public:

		typedef enum {NA,EQ,LE,LT,GT,GE,NE} cmp_t;

	private:
		base_statement* l;
		base_statement* r;

		cmp_t _cmp;

	public:

        virtual bool semantic(){return true;}
        
        virtual void traverse_and_apply(scratch_area *sa){m_scratch = sa;l->traverse_and_apply(m_scratch);r->traverse_and_apply(m_scratch);}

		virtual void print(int ident){
			std::cout  << std::string(ident,' ') << "arithmetic_operand:" << _cmp <<":";l->print(ident-5);r->print(ident-5);
		}

		virtual int eval(){
			switch (_cmp)
			{
				case EQ:
					return l->eval() == r->eval();
					break;

				case LE:
					return l->eval() <= r->eval();
					break;

				case GE:
					return l->eval() >= r->eval();
					break;

				case NE:
					return l->eval() != r->eval();
					break;

				case GT:
					return l->eval() > r->eval();
					break;

				case LT:
					return l->eval() < r->eval();
					break;
				default:
					throw base_s3select_exception("wrong compare");
					break;
			}
		}

		arithmetic_operand(base_statement*_l , cmp_t c , base_statement* _r):l(_l),r(_r),_cmp(c){}
};

class logical_operand : public base_statement  {

	public:

		typedef enum {AND,OR,NA} oplog_t;

	private:
		base_statement* l;
		base_statement* r;

		oplog_t _oplog;

	public:

        virtual void traverse_and_apply(scratch_area *sa){m_scratch = sa;l->traverse_and_apply(m_scratch);r->traverse_and_apply(m_scratch);}

        virtual bool semantic(){return true;}

		logical_operand(base_statement *_l , oplog_t _o ,base_statement* _r):l(_l),r(_r),_oplog(_o){}

		virtual void print(int ident){
			std::cout << std::string(ident,' ')<< "logical_operand:" << _oplog <<":\n";l->print(ident-5);r->print(ident-5);
			std::cout << std::endl;
		}

		virtual int eval()
		{
			if (_oplog == NA)
			{
				if (l)
					return l->eval();
				else if (r)
					return r->eval();
			}
			else if (_oplog == AND)
			{
				return (l->eval() && r->eval());
			}
			else
			{
				return (l->eval() || r->eval());
			}
		}
};

class mulldiv_operation : public base_statement {

	public:

		typedef enum {NA,MULL,DIV,POW} muldiv_t;

	private:
		base_statement* l;
		base_statement* r;

		muldiv_t _mulldiv;

	public:

        virtual void traverse_and_apply(scratch_area *sa){m_scratch = sa;l->traverse_and_apply(m_scratch);r->traverse_and_apply(m_scratch);}

        virtual bool semantic(){return true;}

		virtual void print(int ident){
			std::cout  << std::string(ident,' ') << "mulldiv_operation:" << _mulldiv <<":";l->print(ident-5);r->print(ident-5);
		}

		virtual int eval(){
			switch  (_mulldiv)
			{
				case MULL:
					return l->eval() * r->eval();
					break;

				case DIV:
					return l->eval() / r->eval();
					break;

                		case POW:
                    			{int res=1;for(int i=0;i++<r->eval();res*=l->eval());
                    			return res;}
                    			break;

				default:
					throw base_s3select_exception("wrong operator for mul-div");
					break;
			}
		}

		mulldiv_operation(base_statement*_l , muldiv_t c , base_statement* _r):l(_l),r(_r),_mulldiv(c){}
};

class addsub_operation : public base_statement  {

	public:

		typedef enum {ADD,SUB,NA} addsub_op_t;

	private:
		base_statement* l;
		base_statement* r;

		addsub_op_t _op;

	public:

        virtual void traverse_and_apply(scratch_area *sa){m_scratch = sa;l->traverse_and_apply(m_scratch);r->traverse_and_apply(m_scratch);}

        virtual bool semantic(){return true;}

		addsub_operation(base_statement *_l , addsub_op_t _o ,base_statement* _r):l(_l),r(_r),_op(_o){}

		virtual void print(int ident){
			std::cout << std::string(ident,' ')<< "addsub_operation:" << _op <<":\n";l->print(ident-5);r->print(ident-5);
			std::cout << std::endl;
		}

		virtual int eval()
		{
			if (_op == NA) // -num , +num , unary-operation on number 
			{
				if (l)
					return l->eval();
				else if (r)
					return r->eval();
			}
			else if (_op == ADD)
			{
				return (l->eval() + r->eval());
			}
			else
			{
				return (l->eval() - r->eval());
			}
		}
};

class __function : public base_statement {

    private:

        list<base_statement*> arguments;
        list<variable*> args_results;
        std::string name;
        //TODO _func_impl + arguments 

    public:

        virtual void traverse_and_apply(scratch_area *sa)
        {m_scratch = sa;
            for(base_statement* ba : arguments){
                ba->traverse_and_apply(sa);
            }
        }

        virtual bool semantic(){return true;}

        __function(const char * fname):name(fname){} //TODO register to real function(?) , semantic(?)

        virtual int eval()
        {int res = 0;
            //looping and eval per each.
            for(base_statement* ba : arguments)
            {
                args_results.push_back( new variable(ba->eval()) );

                res += ba->eval();//simulate function body
            }
            //pushing / calling to "real __function" bind with all evaluated argumnets
            return res;
        }
        
        virtual void print(int ident){}

        void push_argument(base_statement * arg)
        {
            arguments.push_back(arg);
        }
};
