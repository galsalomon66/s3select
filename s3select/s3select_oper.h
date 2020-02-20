#ifndef __S3SELECT_OPER__
#define __S3SELECT_OPER__

#include <string>
#include <iostream>
#include <list>
#include <map>
#include <string.h>
#include <math.h>

using namespace std;

class base_s3select_exception
{

public:
    enum class s3select_exp_en_t
    {
        NONE,
        ERROR,
        FATAL
    } ;

private:
    s3select_exp_en_t m_severity;

public:
    const char *_msg;
    base_s3select_exception(const char *n) : m_severity(s3select_exp_en_t::NONE) { _msg = n; }
    base_s3select_exception(const char *n, s3select_exp_en_t severity) : m_severity(severity) { _msg = n; }
    base_s3select_exception(std::string n, s3select_exp_en_t severity) : m_severity(severity) { _msg = n.c_str(); } 

    virtual const char *what() { return _msg; }

    s3select_exp_en_t severity() { return m_severity; }

    virtual ~base_s3select_exception() {}
};


// pointer to dynamic allocated buffer , which used for placement new.
static __thread char* _s3select_buff_ptr =0;

class s3select_allocator //s3select is the "owner"
{
    private:

    list<char*> list_of_buff;
    u_int32_t m_idx;

    public:
        #define __S3_ALLOCATION_BUFF__ (8*1024) 
        s3select_allocator():m_idx(0)
        {
            list_of_buff.push_back((char*)malloc(__S3_ALLOCATION_BUFF__));
        }

        void set_global_buff()
        {
            char * buff = list_of_buff.back();
            _s3select_buff_ptr = &buff[ m_idx ];
        }

        void check_capacity(size_t sz)
        {
            if (sz>__S3_ALLOCATION_BUFF__)
                throw base_s3select_exception("requested size too big",base_s3select_exception::s3select_exp_en_t::FATAL);

            if ((m_idx + sz) >= __S3_ALLOCATION_BUFF__)
            {
                list_of_buff.push_back((char *)malloc(__S3_ALLOCATION_BUFF__));
                m_idx = 0;
            }
        }

        void inc(size_t sz)
        {
            m_idx += sz;
            m_idx += sizeof(char*) - (m_idx % sizeof(char*)); //alignment
        }

        void zero()
        {//not a must, its for safty.
            _s3select_buff_ptr=0;
        }
        
        virtual ~s3select_allocator()
        {
            for(auto b : list_of_buff)
                free(b);
        }
};

class __clt_allocator
{
    public:
        s3select_allocator * m_s3select_allocator;

    public:
        
        __clt_allocator():m_s3select_allocator(0){}

        void set(s3select_allocator * a)
        {
            m_s3select_allocator = a;
        }
};

// placement new for allocation of all s3select objects on single(or few) buffers, deallocation of those objects is by releasing the buffer.
#define S3SELECT_NEW( type , ... ) [=]() \
        {   \
            m_s3select_allocator->check_capacity(sizeof( type )); \
            m_s3select_allocator->set_global_buff(); \
            auto res=new (_s3select_buff_ptr) type(__VA_ARGS__); \
            m_s3select_allocator->inc(sizeof( type )); \
            m_s3select_allocator->zero(); \
            return res; \
        }();

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
        if (num_of_tokens > (int)(sizeof(m_columns)/sizeof(char*))) 
                throw base_s3select_exception("too_many_tokens");

        m_upper_bound = num_of_tokens;

        for (int i = 0; i < num_of_tokens; i++)
        {
            m_columns[i] = tokens[i];
        }
        //TODO m_columns[i]=0;
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

    const char* get_column_value(int column_pos)
    {
    
        if ((column_pos >= m_upper_bound) || column_pos < 0) 
            throw base_s3select_exception("column_position_is_wrong",base_s3select_exception::s3select_exp_en_t::ERROR); 

        return m_columns[column_pos];
    }

    int get_num_of_columns(){
        return m_upper_bound;
    }
};

struct binop_plus {
    double operator()(double a,double b)
    {
        return a+b;
    }
};

struct binop_minus {
    double operator()(double a,double b)
    {
        return a-b;
    }
};

struct binop_mult {
    double operator()(double a,double b)
    {
        return a * b;
    }
};

struct binop_div {
    double operator()(double a,double b)
    {
        return a / b;
    }
};

struct binop_pow {
    double operator()(double a,double b)
    {
        return pow(a,b);
    }
};

class value 
{

public:
    typedef union {
        int64_t num;
        const char *str;
        double dbl;
    } value_t;

private:
    value_t __val;

public:
    enum class value_En_t
    {
        DECIMAL,
        FLOAT,
        STRING,
        NA
    } ;
    value_En_t type; // TODO private

    value(int64_t n) : type(value_En_t::DECIMAL) { __val.num = n; }
    value(int n) : type(value_En_t::DECIMAL) { __val.num = n; }
    value(bool b) : type(value_En_t::DECIMAL) { __val.num = (int64_t)b; }
    value(double d) : type(value_En_t::FLOAT) { __val.dbl = d; }
    value(const char *s) : type(value_En_t::STRING) { __val.str = s; } //must be allocated "all the way up the stack (AST)"
    value():type(value_En_t::NA){__val.num=0;}

    bool is_number() const
    {
        if ((type != value_En_t::STRING))
            return true;
        else
            return false;
    }
    bool is_string() const { return type == value_En_t::STRING; }

    int64_t get_num(){return __val.num;}

    std::string to_string(){//TODO very intensive , must improve this

        if (type != value_En_t::STRING){
                if (type == value_En_t::DECIMAL){
                        return std::to_string(__val.num);
                }else {
                        return std::to_string(__val.dbl);
                }
        }else{
            return __val.str;
        }
    }

    value & operator=(const value & o)
    {
        this->__val = o.__val;
        this->type = o.type;

	return *this;
    }

    int64_t i64()
    {
        return __val.num;
    }

    const char * str()
    {
        return __val.str;
    }

    double dbl()
    {
        return __val.dbl;
    }

    bool operator<(const value &v)//basic compare operator , most itensive runtime operation 
    {
            //TODO NA possible?
        if (is_string() && v.is_string())
            return strcmp(__val.str, v.__val.str) < 0;
        
        if (is_number() && v.is_number()){

            if(type != v.type){ //conversion //TODO find better way
                    if (type == value_En_t::DECIMAL){
                            return (double)__val.num < v.__val.dbl;
                    }
                    else {
                            return __val.dbl < (double)v.__val.num;
                    }
            }
            else { //no conversion
                if(type == value_En_t::DECIMAL){
                            return __val.num < v.__val.num;
                }
                else{
                            return __val.dbl < v.__val.dbl;
                }
                
            }
        }

        throw base_s3select_exception("operands not of the same type(numeric , string), while comparision");
    }

    //intensive runtime operations
    friend bool operator<(const value &l , const value &r) // need a friend ...
    {
            return (value)l<(value)r; //TODO reolve the segfault (remove casting)
    }

    bool operator>(const value &v)  {return v < *this;}
    bool operator<=(const value &v)  {return !(*this>v);}
    bool operator>=(const value &v)  {return !(*this<v);}
    bool operator==(const value &v)  {return !(*this<v) && !(*this>v);} //TODO not efficient. need specific implementation
    bool operator!=(const value &v)  {return !(*this == v);}

    template<typename binop> //conversion rules for arithmetical binary operations 
        value &compute(value &l,const value &r)//left should be this, it contain the result
    {
        binop __op;

        if (l.is_string() || r.is_string())
            throw base_s3select_exception("illegal binary operation with string");

        if (l.type != r.type)
        { //conversion

            if (l.type == value_En_t::DECIMAL)
            {
                l.__val.dbl = __op((double)l.__val.num , r.__val.dbl);
                l.type = value_En_t::FLOAT;
            }
            else
            {
                l.__val.dbl = __op(l.__val.dbl , (double)r.__val.num);
                l.type = value_En_t::FLOAT;
            }
        }
        else
        { //no conversion

            if (l.type == value_En_t::DECIMAL)
            {
                l.__val.num = __op(l.__val.num , r.__val.num );
                l.type = value_En_t::DECIMAL;
            }
            else
            {
                l.__val.dbl = __op(l.__val.dbl , r.__val.dbl );
                l.type = value_En_t::FLOAT;
            }
        }

        return l;
    }

    value & operator+(const value &v)
    {
        return compute<binop_plus>(*this,v);
    }

    value & operator-(const value &v)
    {
        return compute<binop_minus>(*this,v);
    }

    value & operator*(const value &v)
    {
        return compute<binop_mult>(*this,v);
    }

    value & operator/(const value &v) // TODO  handle division by zero
    {
        return compute<binop_div>(*this,v);
    }

    value & operator^(const value &v)
    {
        return compute<binop_pow>(*this,v);
    }

};

class base_statement  {

    protected:

    scratch_area *m_scratch;
    bool is_last_call; //valid only for aggregation functions

	public:
        base_statement():m_scratch(0),is_last_call(false){}
        virtual value eval() =0;
        virtual base_statement* left() {return 0;}
        virtual base_statement* right() {return 0;}       
		virtual std::string print(int ident) =0;//TODO complete it, one option to use level parametr in interface , 
        virtual bool semantic() =0;//done once , post syntax , traverse all nodes and validate semantics. 
        virtual void traverse_and_apply(scratch_area *sa)
        {
            m_scratch = sa;
            if (left())
                left()->traverse_and_apply(m_scratch);
            if (right())
                right()->traverse_and_apply(m_scratch);
        }

        virtual bool is_aggregate(){return false;}
        virtual bool is_column(){return false;}
        
        bool is_function();
        bool is_aggregate_exist_in_expression(base_statement* e);//TODO obsolete ?
        base_statement* get_aggregate();
        bool is_nested_aggregate(base_statement *e);
        bool is_binop_aggregate_and_column(base_statement*skip);

        virtual void set_last_call()
        {
            is_last_call = true;
            if(left()) left()->set_last_call();
            if(right()) right()->set_last_call();
        }

        bool is_set_last_call(){return is_last_call;}

        virtual ~base_statement(){}

};

class variable : public base_statement
{

public:

    enum class var_t
    {
        NA,
        VAR,//schema column (i.e. age , price , ...)
        COL_VALUE, //concrete value
        POS, // CSV column number  (i.e. _1 , _2 ... )
        STAR_OPERATION, //'*'
    } ; 
    var_t m_var_type;

private:
    
    std::string _name;
    int column_pos;
    value var_value;
    std::string m_star_op_result;

public:
    
    //variable():m_var_type(NA),_name("#"){}

    variable(int64_t i) : m_var_type(var_t::COL_VALUE), _name("#"), column_pos(-1),var_value(i){}

    variable(double d) : m_var_type(var_t::COL_VALUE), _name("#"), column_pos(-1),var_value(d){}

    variable(int i) : m_var_type(var_t::COL_VALUE), _name("#"), column_pos(-1),var_value(i){}

    variable(const std::string & n) : m_var_type(var_t::VAR), _name(n), column_pos(-1){}

    variable(const std::string & n ,  var_t tp) : m_var_type(var_t::NA)
    {
        if(tp == variable::var_t::POS)
        {
            _name = n;
            m_var_type = tp;
            int pos = atoi( n.c_str() + 1 ); //TODO >0 < (schema definition , semantic analysis)
            column_pos = pos -1;// _1 is the first column ( zero position )
        }
        else if (tp == variable::var_t::COL_VALUE)
        {
            _name = "#";
            m_var_type = tp;
            column_pos = -1;
            var_value = value(n.c_str());

        }else if (tp ==variable::var_t::STAR_OPERATION)
        {
            _name = "#";
            m_var_type = tp;
            column_pos = -1;
            var_value = value();

        }
    }

    void operator=(const value & v)
    {
        var_value = v;
    }

    virtual ~variable(){}

    virtual bool is_column() {//is reference to column.
            if(m_var_type == var_t::VAR || m_var_type == var_t::POS) return true;
            return false;
    }

    value & get_value() {return var_value;} //TODO is it correct
    virtual value::value_En_t get_value_type() {return var_value.type;}


    const char * star_operation(){ //purpose return content of all columns in a input stream

        m_star_op_result.clear();

	int i;
        int num_of_columns = m_scratch->get_num_of_columns();
        for(i=0;i<num_of_columns-1;i++)
        {
            m_star_op_result += std::string(m_scratch->get_column_value(i)) + ',' ;
        }
        m_star_op_result += std::string(m_scratch->get_column_value(i)) ;

        return m_star_op_result.c_str();
    }

    virtual value eval()
    {
        if (m_var_type == var_t::COL_VALUE) //return value
            return var_value;           // could be deciml / float / string ; its input stream
        else if(m_var_type == var_t::STAR_OPERATION)
            return star_operation();
        else if (column_pos == -1)
            column_pos = m_scratch->get_column_pos(_name.c_str()); //done once , for the first time

        return value(m_scratch->get_column_value(column_pos));//no allocation. returning pointer of allocated space 
    }

    virtual std::string print(int ident)
    {
        //std::string out = std::string(ident,' ') + std::string("var:") + std::to_string(var_value.__val.num);
        //return out;
        return std::string("#");//TBD
    }

    virtual bool semantic(){return false;}

};

class arithmetic_operand : public base_statement {

	public:

		enum class cmp_t {NA,EQ,LE,LT,GT,GE,NE} ;

	private:
		base_statement* l;
		base_statement* r;

		cmp_t _cmp;

	public:

        virtual bool semantic(){return true;}
        
        virtual base_statement* left(){return l;}
        virtual base_statement* right(){return r;}
        
	virtual std::string print(int ident)
    {
           //std::string out = std::string(ident,' ') + "compare:" += std::to_string(_cmp) + "\n" + l->print(ident-5) +r->print(ident+5);
            //return out;
            return std::string("#");//TBD
	}
        
        virtual value eval(){

			switch (_cmp)
			{
				case cmp_t::EQ:
					return (l->eval() == r->eval());
					break;

				case cmp_t::LE:
					return (l->eval() <= r->eval());
					break;

				case cmp_t::GE:
					return (l->eval() >= r->eval());
					break;

				case cmp_t::NE:
					return (l->eval() != r->eval());
					break;

				case cmp_t::GT:
					return (l->eval() > r->eval());
					break;

				case cmp_t::LT:
					return (l->eval() < r->eval());
					break;

				default:
					throw base_s3select_exception("internal error");
					break;
			}
        }

		arithmetic_operand(base_statement*_l , cmp_t c , base_statement* _r):l(_l),r(_r),_cmp(c){}

        virtual ~arithmetic_operand(){}
};

class logical_operand : public base_statement  {

	public:

        enum class oplog_t {AND,OR,NA};

	private:
		base_statement* l;
		base_statement* r;

		oplog_t _oplog;

	public:

        virtual base_statement* left(){return l;}
        virtual base_statement* right(){return r;}

        virtual bool semantic(){return true;}

		logical_operand(base_statement *_l , oplog_t _o ,base_statement* _r):l(_l),r(_r),_oplog(_o){}

        virtual ~logical_operand(){}

        virtual std::string print(int ident)
        {
            //std::string out = std::string(ident, ' ') + "logical_operand:" += std::to_string(_oplog) + "\n" + l->print(ident - 5) + r->print(ident + 5);
            //return out;
            return std::string("#");//TBD
        }
        virtual value eval()
        {
            if (_oplog == oplog_t::AND)
			{
                if (!l || !r) throw base_s3select_exception("missing operand for logical and",base_s3select_exception::s3select_exp_en_t::FATAL);
				return value( (l->eval().get_num() && r->eval().get_num()) );
			}
			else
			{
                if (!l || !r) throw base_s3select_exception("missing operand for logical or",base_s3select_exception::s3select_exp_en_t::FATAL);
				return value( (l->eval().get_num() || r->eval().get_num()) );
			}
        }

};

class mulldiv_operation : public base_statement {

	public:

		enum class muldiv_t {NA,MULL,DIV,POW} ;

	private:
		base_statement* l;
		base_statement* r;

		muldiv_t _mulldiv;

	public:

        virtual base_statement* left(){return l;}
        virtual base_statement* right(){return r;}

        virtual bool semantic(){return true;}

        virtual std::string print(int ident)
        {
            //std::string out = std::string(ident, ' ') + "mulldiv_operation:" += std::to_string(_mulldiv) + "\n" + l->print(ident - 5) + r->print(ident + 5);
            //return out;
            return std::string("#");//TBD
        }

        virtual value eval()
        {
            switch (_mulldiv)
            {
            case muldiv_t::MULL:
                return l->eval() * r->eval();
                break;

            case muldiv_t::DIV:
                return l->eval() / r->eval();
                break;

            case muldiv_t::POW:
                return l->eval() ^ r->eval();
                break;

            default:
		throw base_s3select_exception("internal error");
                break;
            }
        }

		mulldiv_operation(base_statement*_l , muldiv_t c , base_statement* _r):l(_l),r(_r),_mulldiv(c){}

        virtual ~mulldiv_operation(){}
};

class addsub_operation : public base_statement  {

	public:

		enum class addsub_op_t {ADD,SUB,NA};

	private:
		base_statement* l;
		base_statement* r;

		addsub_op_t _op;

	public:

        virtual base_statement* left(){return l;}
        virtual base_statement* right(){return r;}

        virtual bool semantic(){return true;}

		addsub_operation(base_statement *_l , addsub_op_t _o ,base_statement* _r):l(_l),r(_r),_op(_o){}

        virtual ~addsub_operation(){}

        virtual std::string print(int ident)
        {
            //std::string out = std::string(ident, ' ') + "addsub_operation:" += std::to_string(_op) + "\n" + l->print(ident - 5) + r->print(ident + 5);
            return std::string("#");//TBD
        }

        virtual value eval()
        {
            if (_op == addsub_op_t::NA) // -num , +num , unary-operation on number
            {
                if (l)
                    return l->eval();
                else if (r)
                    return r->eval();
            }
            else if (_op == addsub_op_t::ADD)
            {
                return (l->eval() + r->eval()); 
            }
            else
            {
                return (l->eval() - r->eval());
            }
	
		return value();
        }
};

class base_function 
{

protected:
    bool aggregate;

public:
    //TODO bool semantic() validate number of argument and type
    virtual bool operator()(list<base_statement *> *args, variable *result) = 0;
    base_function() : aggregate(false) {}
    bool is_aggregate() { return aggregate == true; }
    virtual void get_aggregate_result(variable *) {}

    virtual ~base_function(){}
};

 enum class s3select_func_En_t {ADD,SUM,MIN,MAX,COUNT,TO_INT,TO_FLOAT,SUBSTR};

struct _fn_add : public base_function{

    bool operator()(list<base_statement*> * args,variable * result)
    {
        list<base_statement*>::iterator iter = args->begin();
        base_statement* x =  *iter;
        iter++;
        base_statement* y = *iter;

        value res = x->eval() + y->eval();
        
        *result = res; 

        return true;
    }
};

struct _fn_sum : public base_function
{

    value sum;

    _fn_sum() : sum(0) { aggregate = true; }

    bool operator()(list<base_statement *> *args, variable *result)
    {
        list<base_statement *>::iterator iter = args->begin();
        base_statement *x = *iter;

        try
        {
            sum = sum + x->eval();
        }
        catch (base_s3select_exception &e)
        {
            std::cout << "illegal value for aggregation(sum). skipping." << std::endl;
            if (e.severity() == base_s3select_exception::s3select_exp_en_t::FATAL)
                throw;
        }

        return true;
    }

    virtual void get_aggregate_result(variable *result) { *result = sum ;} 
};

struct _fn_count : public base_function{

    int64_t count;

    _fn_count():count(0){aggregate=true;}

    bool operator()(list<base_statement*> * args,variable * result)
    {
        count += 1;

        return true;
    }

    virtual void get_aggregate_result(variable*result){ *result = value(count);}
    
};

struct _fn_min : public base_function{

    value min;

    _fn_min():min(__INT64_MAX__){aggregate=true;}

    bool operator()(list<base_statement*> * args,variable * result)
    {
        list<base_statement*>::iterator iter = args->begin();
        base_statement* x =  *iter;

        if(min > x->eval()) min=x->eval();

        return true;
    }

    virtual void get_aggregate_result(variable*result){ *result = min;}
    
};

struct _fn_max : public base_function{

    value max;

    _fn_max():max(-__INT64_MAX__){aggregate=true;}

    bool operator()(list<base_statement*> * args,variable * result)
    {
        list<base_statement*>::iterator iter = args->begin();
        base_statement* x =  *iter;

        if(max < x->eval()) max=x->eval();

        return true;
    }

    virtual void get_aggregate_result(variable*result){*result = max;}
    
};

struct _fn_to_int : public base_function{

    bool operator()(list<base_statement*> * args,variable * result)
    {
        char *perr;
        int64_t i=0;
        value v = (*args->begin())->eval();

        if (v.type == value::value_En_t::STRING)
                i = strtol(v.str() ,&perr ,10) ;//TODO check error before constructor
        else
        if (v.type == value::value_En_t::FLOAT)
                i = v.dbl();
        else
                i = v.i64();
        
        value res = value( i );
        *result = res ;

        return true;
    }
    
};

struct _fn_to_float : public base_function{

    bool operator()(list<base_statement*> * args,variable * result)
    {
        char *perr;
        double d=0;
        value v = (*args->begin())->eval();

        if (v.type == value::value_En_t::STRING)
                d = strtod(v.str() ,&perr) ;//TODO check error before constructor
        else
        if (v.type == value::value_En_t::FLOAT)
                d = v.dbl();
        else
                d = v.i64();
        
        value res = value( d );
        *result = res;

        return true;
    }
    
};

struct _fn_substr : public base_function{

    char buff[4096];// this buffer is persist for the query life time, it use for the results per row(only for the specific function call)
    //it prevent from intensive use of malloc/free (fragmentation).
    //should validate result length.
    //TODO may replace by std::string (dynamic) , or to replace with global allocator , in query scope.

    bool operator()(list<base_statement*> * args,variable * result)
    {
        list<base_statement*>::iterator iter = args->begin();
        int args_size = args->size();


        if (args_size<2)
            throw base_s3select_exception("substr accept 2 arguments or 3");

        base_statement* str =  *iter;
        iter++;
        base_statement* from = *iter;
        base_statement* to;

        if (args_size == 3)
                {
            iter++;
            to = *iter;
        }

        value v_str = str->eval();

        if(v_str.type != value::value_En_t::STRING)
            throw base_s3select_exception("substr first argument must be string");//can skip current row

        int str_length = strlen(v_str.str());

        value v_from = from->eval();
        if(v_from.is_string())
                    throw base_s3select_exception("substr second argument must be number");//can skip current row

        value v_to;
        int64_t f;
        int64_t t;

        if (args_size==3){
            v_to = to->eval();
            if (v_to.is_string())
                throw base_s3select_exception("substr third argument must be number");//can skip row
        }
        
        if (v_from.type == value::value_En_t::FLOAT)
            f=v_from.dbl();
        else
            f=v_from.i64();

        if (f>str_length)
            throw base_s3select_exception("substr start position is too far");//can skip row

        if (str_length>(int)sizeof(buff))
            throw base_s3select_exception("string too long for internal buffer");//can skip row

        if (args_size == 3)
        {
            if (v_from.type == value::value_En_t::FLOAT)
                t = v_to.dbl();
            else
                t = v_to.i64();

            if( (str_length-(f-1)-t) <0)
                throw base_s3select_exception("substr length parameter beyond bounderies");//can skip row

            strncpy(buff,v_str.str()+f-1,t);
        }
        else 
            strcpy(buff,v_str.str()+f-1);
        
        *result = value(buff);

        return true;
    }
    
};

class s3select_functions : public __clt_allocator {

    private:
        
        std::map<std::string,s3select_func_En_t> m_functions_library;

        void build_library()
        {
            // s3select function-name (string) --> function Enum
            m_functions_library.insert(pair<std::string,s3select_func_En_t>("add",s3select_func_En_t::ADD) );
            m_functions_library.insert(pair<std::string,s3select_func_En_t>("sum",s3select_func_En_t::SUM) );
            m_functions_library.insert(pair<std::string,s3select_func_En_t>("count",s3select_func_En_t::COUNT) );
            m_functions_library.insert(pair<std::string,s3select_func_En_t>("min",s3select_func_En_t::MIN) );
            m_functions_library.insert(pair<std::string,s3select_func_En_t>("max",s3select_func_En_t::MAX) );
            m_functions_library.insert(pair<std::string,s3select_func_En_t>("int",s3select_func_En_t::TO_INT) );
            m_functions_library.insert(pair<std::string,s3select_func_En_t>("float",s3select_func_En_t::TO_FLOAT) );
            m_functions_library.insert(pair<std::string,s3select_func_En_t>("substr",s3select_func_En_t::SUBSTR) );
        }

    public:

        s3select_functions()
        {
            build_library();
        }

    base_function * create(std::string fn_name)
    {
        std::map<std::string,s3select_func_En_t>::iterator iter = m_functions_library.find(fn_name);

        if (iter == m_functions_library.end())
        {
            std::string msg;
            msg = fn_name + " " + " function not found";
            throw base_s3select_exception(msg, base_s3select_exception::s3select_exp_en_t::FATAL);
        }

        switch( iter->second )
        {
            case s3select_func_En_t::ADD:
                return S3SELECT_NEW (_fn_add);
            break;

            case s3select_func_En_t::SUM:
                return S3SELECT_NEW(_fn_sum);
            break;

            case s3select_func_En_t::COUNT:
                return S3SELECT_NEW(_fn_count);
            break;

            case s3select_func_En_t::MIN:
                return S3SELECT_NEW(_fn_min);
            break;

            case s3select_func_En_t::MAX:
                return S3SELECT_NEW(_fn_max);
            break;

            case s3select_func_En_t::TO_INT:
                return S3SELECT_NEW(_fn_to_int);
            break;

            case s3select_func_En_t::TO_FLOAT:
                return S3SELECT_NEW(_fn_to_float);
            break;

            case s3select_func_En_t::SUBSTR:
                return S3SELECT_NEW(_fn_substr);
            break;

            default:
                throw base_s3select_exception("internal error while resolving function-name");
            break;
        }
    }
};


class __function : public base_statement
{

private:
    list<base_statement *> arguments;
    std::string name;
    base_function *m_func_impl;
    s3select_functions *m_s3select_functions;

    void _resolve_name()
    {
        if (m_func_impl)
            return;

        base_function *f = m_s3select_functions->create(name);
        if (!f)
            throw base_s3select_exception("function not found", base_s3select_exception::s3select_exp_en_t::FATAL); //should abort query
        m_func_impl = f;
    }

public:
    virtual void traverse_and_apply(scratch_area *sa)
    {
        m_scratch = sa;
        for (base_statement *ba : arguments)
        {
            ba->traverse_and_apply(sa);
        }
    }

    virtual bool is_aggregate() // TODO under semantic flow
    {
        _resolve_name();

        return m_func_impl->is_aggregate();
    }

    virtual bool semantic() { return true; }

    __function(const char *fname, s3select_functions* s3f) : name(fname), m_func_impl(0),m_s3select_functions(s3f) {}

    virtual value eval(){

        _resolve_name();

        variable result(0); //TODO create variable::NA
        if (is_last_call == false)
            (*m_func_impl)(&arguments, &result);
        else
            (*m_func_impl).get_aggregate_result(&result);

        return result.get_value();
    }



    virtual std::string  print(int ident) {return std::string(0);}

    void push_argument(base_statement *arg)
    {
        arguments.push_back(arg);
    }


    list<base_statement *> get_arguments()
    {
        return arguments;
    }

    virtual ~__function() {arguments.clear();}
};

bool base_statement::is_function()
{
    if (dynamic_cast<__function *>(this))
        return true;
    else
        return false;
}

bool base_statement::is_aggregate_exist_in_expression(base_statement *e) //TODO obsolete ?
{
    if (e->is_aggregate())
        return true;

    if (e->left() && e->left()->is_aggregate_exist_in_expression(e->left()))
        return true;

    if (e->right() && e->right()->is_aggregate_exist_in_expression(e->right()))
        return true;

    if (e->is_function())
    {
        for (auto i : dynamic_cast<__function *>(e)->get_arguments())
            if (e->is_aggregate_exist_in_expression(i))
                return true;
    }

    return false;
}

base_statement *base_statement::get_aggregate()
{//search for aggregation function in AST
    base_statement * res = 0;

    if (is_aggregate())
        return this;

    if (left() && (res=left()->get_aggregate())!=0) return res;

    if (right() && (res=right()->get_aggregate())!=0) return res;

    if (is_function())
    {
        for (auto i : dynamic_cast<__function *>(this)->get_arguments())
        {
            base_statement* b=i->get_aggregate();
            if (b) return b;
        }
    }
    return 0;
}

bool base_statement::is_nested_aggregate(base_statement *e) 
{//validate for non nested calls for aggregation function, i.e. sum ( min ( ))
    if (e->is_aggregate())
    {
        if (e->left())
        {
            if (e->left()->is_aggregate_exist_in_expression(e->left()))
                return true;
        }
        else if (e->right())
        {
            if (e->right()->is_aggregate_exist_in_expression(e->right()))
                return true;
        }
        else if (e->is_function())
        {
            for (auto i : dynamic_cast<__function *>(e)->get_arguments())
            {
                if (i->is_aggregate_exist_in_expression(i)) return true;
            }
        }
        return false;
    }
    return false;
}

// select sum(c2) ... + c1 ... is not allowed. a binary operation with scalar is OK. i.e. select sum() + 1
bool base_statement::is_binop_aggregate_and_column(base_statement *skip_expression)
{
    if (left() && left() != skip_expression) //can traverse to left
    {
        if (left()->is_column())
            return true;
        else
            if (left()->is_binop_aggregate_and_column(skip_expression) == true) return true;
    }
    
    if (right() && right() != skip_expression) //can traverse right
    {
        if (right()->is_column())
            return true;
        else
            if (right()->is_binop_aggregate_and_column(skip_expression) == true) return true;
    }

    if (this != skip_expression && is_function())
    {

        __function* f = (dynamic_cast<__function *>(this));
        list<base_statement*> l = f->get_arguments();
        for (auto i : l)
        {
            if (i!=skip_expression && i->is_column())
                return true;
            if (i->is_binop_aggregate_and_column(skip_expression) == true) return true;
        }
    }

    return false;
}

#endif
