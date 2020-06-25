#ifndef __S3SELECT_OPER__
#define __S3SELECT_OPER__

#include <string>
#include <iostream>
#include <list>
#include <map>
#include <vector>
#include <string.h>
#include <math.h>

#include <boost/lexical_cast.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/bind.hpp>
namespace bsc = BOOST_SPIRIT_CLASSIC_NS;

namespace s3selectEngine
{

//=== stl allocator definition
//this allocator is fit for placement new (no calls to heap)

class chunkalloc_out_of_mem
{
};

template <typename T, size_t pool_sz>
class ChunkAllocator : public std::allocator<T>
{
public:
  typedef size_t size_type;
  typedef T* pointer;
  typedef const T* const_pointer;
  size_t __pool_loc;
  char* __pool;

  //only ONE pool,not allocated dynamically; main assumption, caller knows in advance its memory limitations.
  char __buff__[pool_sz];

  template <typename _Tp1>
  struct rebind
  {
    typedef ChunkAllocator<_Tp1, pool_sz> other;
  };

  //==================================
  inline T* _Allocate(size_t _Count, T*)
  {
    // allocate storage for _Count elements of type T

    pointer res = (pointer)(__pool + __pool_loc);

    __pool_loc += sizeof(T) * _Count;
    //alignment
    size_t _algn = ((size_t)(__pool_loc) % sizeof(char*));
    __pool_loc += _algn != 0 ? sizeof(char*) - _algn : 0;

    if (__pool_loc > sizeof(__buff__))
    {
      throw chunkalloc_out_of_mem();
    }

    return res;
  }

  //==================================
  inline pointer allocate(size_type n, const void* hint = 0)
  {
    return (_Allocate(n, (pointer)0));
  }

  //==================================
  inline void deallocate(pointer p, size_type n)
  {
  }

  //==================================
  ChunkAllocator() throw() : std::allocator<T>()
  {
    // alloc from main-buffer
    __pool_loc = 0;
    memset( &__buff__[0], 0, sizeof(__buff__));
    __pool = &__buff__[0];
  }

  //==================================
  ChunkAllocator(const ChunkAllocator& other) throw() : std::allocator<T>(other)
  {
    // copy const
    __pool_loc = 0;
    __pool = &__buff__[0];
  }

  //==================================
  ~ChunkAllocator() throw()
  {
    //do nothing
  }
};

class base_statement;
//typedef std::vector<base_statement *> bs_stmt_vec_t; //without specific allocator

//ChunkAllocator, prevent allocation from heap.
typedef std::vector<base_statement*, ChunkAllocator<base_statement*, 256> > bs_stmt_vec_t;

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
  std::string _msg;
  base_s3select_exception(const char* n) : m_severity(s3select_exp_en_t::NONE)
  {
    _msg.assign(n);
  }
  base_s3select_exception(const char* n, s3select_exp_en_t severity) : m_severity(severity)
  {
    _msg.assign(n);
  }
  base_s3select_exception(std::string n, s3select_exp_en_t severity) : m_severity(severity)
  {
    _msg = n;
  }

  virtual const char* what()
  {
    return _msg.c_str();
  }

  s3select_exp_en_t severity()
  {
    return m_severity;
  }

  virtual ~base_s3select_exception() {}
};


// pointer to dynamic allocated buffer , which used for placement new.
static __thread char* _s3select_buff_ptr =0;

class s3select_allocator //s3select is the "owner"
{
private:

  std::vector<char*> list_of_buff;
  u_int32_t m_idx;

public:
#define __S3_ALLOCATION_BUFF__ (8*1024)
  s3select_allocator():m_idx(0)
  {
    list_of_buff.push_back((char*)malloc(__S3_ALLOCATION_BUFF__));
  }

  void set_global_buff()
  {
    char* buff = list_of_buff.back();
    _s3select_buff_ptr = &buff[ m_idx ];
  }

  void check_capacity(size_t sz)
  {
    if (sz>__S3_ALLOCATION_BUFF__)
    {
      throw base_s3select_exception("requested size too big", base_s3select_exception::s3select_exp_en_t::FATAL);
    }

    if ((m_idx + sz) >= __S3_ALLOCATION_BUFF__)
    {
      list_of_buff.push_back((char*)malloc(__S3_ALLOCATION_BUFF__));
      m_idx = 0;
    }
  }

  void inc(size_t sz)
  {
    m_idx += sz;
    m_idx += sizeof(char*) - (m_idx % sizeof(char*)); //alignment
  }

  void zero()
  {
    //not a must, its for safty.
    _s3select_buff_ptr=0;
  }

  virtual ~s3select_allocator()
  {
    for(auto b : list_of_buff)
    {
      free(b);
    }
  }
};

class __clt_allocator
{
public:
  s3select_allocator* m_s3select_allocator;

public:

  __clt_allocator():m_s3select_allocator(0) {}

  void set(s3select_allocator* a)
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
  std::vector<std::string_view> m_columns{128};
  int m_upper_bound;

  std::vector<std::pair<std::string, int >> m_column_name_pos;

public:

  void set_column_pos(const char* n, int pos)//TODO use std::string
  {
    m_column_name_pos.push_back( std::pair<const char*, int>(n, pos));
  }

  void update(std::vector<char*>& tokens, size_t num_of_tokens)
  {
    size_t i=0;
    for(auto s : tokens)
    {
      if (i>=num_of_tokens)
      {
        break;
      }

      m_columns[i++] = s;
    }
    m_upper_bound = i;

  }

  int get_column_pos(const char* n)
  {
    //done only upon building the AST , not on "runtime"

    std::vector<std::pair<std::string, int >>::iterator iter;

    for( auto iter : m_column_name_pos)
    {
      if (!strcmp(iter.first.c_str(), n))
      {
        return iter.second;
      }
    }

    return -1;
  }

  std::string_view get_column_value(int column_pos)
  {

    if ((column_pos >= m_upper_bound) || column_pos < 0)
    {
      throw base_s3select_exception("column_position_is_wrong", base_s3select_exception::s3select_exp_en_t::ERROR);
    }

    return m_columns[column_pos];
  }

  int get_num_of_columns()
  {
    return m_upper_bound;
  }
};

class base_statement;
class projection_alias
{
//purpose: mapping between alias-name to base_statement*
//those routines are *NOT* intensive, works once per query parse time.

private:
  std::vector< std::pair<std::string, base_statement*> > alias_map;

public:
  std::vector< std::pair<std::string, base_statement*> >* get()
  {
    return &alias_map;
  }

  bool insert_new_entry(std::string alias_name, base_statement* bs)
  {
    //purpose: only unique alias names.

    for(auto alias: alias_map)
    {
      if(alias.first.compare(alias_name) == 0)
      {
        return false;  //alias name already exist
      }

    }
    std::pair<std::string, base_statement*> new_alias(alias_name, bs);
    alias_map.push_back(new_alias);

    return true;
  }

  base_statement* search_alias(std::string alias_name)
  {
    for(auto alias: alias_map)
    {
      if(alias.first.compare(alias_name) == 0)
      {
        return alias.second;  //refernce to execution node
      }
    }
    return 0;
  }
};

struct binop_plus
{
  double operator()(double a, double b)
  {
    return a+b;
  }
};

struct binop_minus
{
  double operator()(double a, double b)
  {
    return a-b;
  }
};

struct binop_mult
{
  double operator()(double a, double b)
  {
    return a * b;
  }
};

struct binop_div
{
  double operator()(double a, double b)
  {
    return a / b;
  }
};

struct binop_pow
{
  double operator()(double a, double b)
  {
    return pow(a, b);
  }
};

class value
{

public:
  typedef union
  {
    int64_t num;
    char* str;//TODO consider string_view
    double dbl;
    boost::posix_time::ptime* timestamp;
  } value_t;

private:
  value_t __val;
  std::string m_to_string;
  std::string m_str_value;

public:
  enum class value_En_t
  {
    DECIMAL,
    FLOAT,
    STRING,
    TIMESTAMP,
    NA
  } ;
  value_En_t type;

  value(int64_t n) : type(value_En_t::DECIMAL)
  {
    __val.num = n;
  }
  value(int n) : type(value_En_t::DECIMAL)
  {
    __val.num = n;
  }
  value(bool b) : type(value_En_t::DECIMAL)
  {
    __val.num = (int64_t)b;
  }
  value(double d) : type(value_En_t::FLOAT)
  {
    __val.dbl = d;
  }
  value(boost::posix_time::ptime* timestamp) : type(value_En_t::TIMESTAMP)
  {
    __val.timestamp = timestamp;
  }

  value(const char* s) : type(value_En_t::STRING)
  {
    m_str_value.assign(s);
    __val.str = m_str_value.data();
  }

  value():type(value_En_t::NA)
  {
    __val.num=0;
  }

  bool is_number() const
  {
    if ((type == value_En_t::DECIMAL || type == value_En_t::FLOAT))
    {
      return true;
    }

    return false;
  }

  bool is_string() const
  {
    return type == value_En_t::STRING;
  }
  bool is_timestamp() const
  {
    return type == value_En_t::TIMESTAMP;
  }


  std::string& to_string()  //TODO very intensive , must improve this
  {

    if (type != value_En_t::STRING)
    {
      if (type == value_En_t::DECIMAL)
      {
        m_to_string.assign( boost::lexical_cast<std::string>(__val.num) );
      }
      else if(type == value_En_t::FLOAT)
      {
        m_to_string = boost::lexical_cast<std::string>(__val.dbl);
      }
      else if (type == value_En_t::TIMESTAMP)
      {
        m_to_string =  to_simple_string( *__val.timestamp );
      }
    }
    else
    {
      m_to_string.assign( __val.str );
    }

    return m_to_string;
  }


  value& operator=(value& o)
  {
    if(this->type == value_En_t::STRING)
    {
      m_str_value.assign(o.str());
      __val.str = m_str_value.data();
    }
    else
    {
      this->__val = o.__val;
    }

    this->type = o.type;

    return *this;
  }

  value& operator=(const char* s)
  {
    m_str_value.assign(s);
    this->__val.str = m_str_value.data();
    this->type = value_En_t::STRING;

    return *this;
  }

  value& operator=(int64_t i)
  {
    this->__val.num = i;
    this->type = value_En_t::DECIMAL;

    return *this;
  }

  value& operator=(double d)
  {
    this->__val.dbl = d;
    this->type = value_En_t::FLOAT;

    return *this;
  }

  value& operator=(bool b)
  {
    this->__val.num = (int64_t)b;
    this->type = value_En_t::DECIMAL;

    return *this;
  }

  value& operator=(boost::posix_time::ptime* p)
  {
    this->__val.timestamp = p;
    this->type = value_En_t::TIMESTAMP;

    return *this;
  }

  int64_t i64()
  {
    return __val.num;
  }

  const char* str()
  {
    return __val.str;
  }

  double dbl()
  {
    return __val.dbl;
  }

  boost::posix_time::ptime* timestamp() const
  {
    return __val.timestamp;
  }

  bool operator<(const value& v)//basic compare operator , most itensive runtime operation
  {
    //TODO NA possible?
    if (is_string() && v.is_string())
    {
      return strcmp(__val.str, v.__val.str) < 0;
    }

    if (is_number() && v.is_number())
    {

      if(type != v.type)  //conversion //TODO find better way
      {
        if (type == value_En_t::DECIMAL)
        {
          return (double)__val.num < v.__val.dbl;
        }
        else
        {
          return __val.dbl < (double)v.__val.num;
        }
      }
      else   //no conversion
      {
        if(type == value_En_t::DECIMAL)
        {
          return __val.num < v.__val.num;
        }
        else
        {
          return __val.dbl < v.__val.dbl;
        }

      }
    }

    if(is_timestamp() && v.is_timestamp())
    {
      return *timestamp() < *(v.timestamp());
    }

    throw base_s3select_exception("operands not of the same type(numeric , string), while comparision");
  }

  bool operator>(const value& v) //basic compare operator , most itensive runtime operation
  {
    //TODO NA possible?
    if (is_string() && v.is_string())
    {
      return strcmp(__val.str, v.__val.str) > 0;
    }

    if (is_number() && v.is_number())
    {

      if(type != v.type)  //conversion //TODO find better way
      {
        if (type == value_En_t::DECIMAL)
        {
          return (double)__val.num > v.__val.dbl;
        }
        else
        {
          return __val.dbl > (double)v.__val.num;
        }
      }
      else   //no conversion
      {
        if(type == value_En_t::DECIMAL)
        {
          return __val.num > v.__val.num;
        }
        else
        {
          return __val.dbl > v.__val.dbl;
        }

      }
    }

    if(is_timestamp() && v.is_timestamp())
    {
      return *timestamp() > *(v.timestamp());
    }

    throw base_s3select_exception("operands not of the same type(numeric , string), while comparision");
  }

  bool operator==(const value& v) //basic compare operator , most itensive runtime operation
  {
    //TODO NA possible?
    if (is_string() && v.is_string())
    {
      return strcmp(__val.str, v.__val.str) == 0;
    }


    if (is_number() && v.is_number())
    {

      if(type != v.type)  //conversion //TODO find better way
      {
        if (type == value_En_t::DECIMAL)
        {
          return (double)__val.num == v.__val.dbl;
        }
        else
        {
          return __val.dbl == (double)v.__val.num;
        }
      }
      else   //no conversion
      {
        if(type == value_En_t::DECIMAL)
        {
          return __val.num == v.__val.num;
        }
        else
        {
          return __val.dbl == v.__val.dbl;
        }

      }
    }

    if(is_timestamp() && v.is_timestamp())
    {
      return *timestamp() == *(v.timestamp());
    }

    throw base_s3select_exception("operands not of the same type(numeric , string), while comparision");
  }
  bool operator<=(const value& v)
  {
    return !(*this>v);
  }
  bool operator>=(const value& v)
  {
    return !(*this<v);
  }
  bool operator!=(const value& v)
  {
    return !(*this == v);
  }

  template<typename binop> //conversion rules for arithmetical binary operations
  value& compute(value& l, const value& r) //left should be this, it contain the result
  {
    binop __op;

    if (l.is_string() || r.is_string())
    {
      throw base_s3select_exception("illegal binary operation with string");
    }

    if (l.type != r.type)
    {
      //conversion

      if (l.type == value_En_t::DECIMAL)
      {
        l.__val.dbl = __op((double)l.__val.num, r.__val.dbl);
        l.type = value_En_t::FLOAT;
      }
      else
      {
        l.__val.dbl = __op(l.__val.dbl, (double)r.__val.num);
        l.type = value_En_t::FLOAT;
      }
    }
    else
    {
      //no conversion

      if (l.type == value_En_t::DECIMAL)
      {
        l.__val.num = __op(l.__val.num, r.__val.num );
        l.type = value_En_t::DECIMAL;
      }
      else
      {
        l.__val.dbl = __op(l.__val.dbl, r.__val.dbl );
        l.type = value_En_t::FLOAT;
      }
    }

    return l;
  }

  value& operator+(const value& v)
  {
    return compute<binop_plus>(*this, v);
  }

  value& operator-(const value& v)
  {
    return compute<binop_minus>(*this, v);
  }

  value& operator*(const value& v)
  {
    return compute<binop_mult>(*this, v);
  }

  value& operator/(const value& v)  // TODO  handle division by zero
  {
    return compute<binop_div>(*this, v);
  }

  value& operator^(const value& v)
  {
    return compute<binop_pow>(*this, v);
  }

};

class base_statement
{

protected:

  scratch_area* m_scratch;
  projection_alias* m_aliases;
  bool is_last_call; //valid only for aggregation functions
  bool m_is_cache_result;
  value m_alias_result;
  base_statement* m_projection_alias;
  int m_eval_stack_depth;

public:
  base_statement():m_scratch(0), is_last_call(false), m_is_cache_result(false), m_projection_alias(0), m_eval_stack_depth(0) {}
  virtual value& eval() =0;
  virtual base_statement* left()
  {
    return 0;
  }
  virtual base_statement* right()
  {
    return 0;
  }
  virtual std::string print(int ident) =0;//TODO complete it, one option to use level parametr in interface ,
  virtual bool semantic() =0;//done once , post syntax , traverse all nodes and validate semantics.

  virtual void traverse_and_apply(scratch_area* sa, projection_alias* pa)
  {
    m_scratch = sa;
    m_aliases = pa;
    if (left())
    {
      left()->traverse_and_apply(m_scratch, m_aliases);
    }
    if (right())
    {
      right()->traverse_and_apply(m_scratch, m_aliases);
    }
  }

  virtual bool is_aggregate()
  {
    return false;
  }
  virtual bool is_column()
  {
    return false;
  }

  bool is_function();
  bool is_aggregate_exist_in_expression(base_statement* e);//TODO obsolete ?
  base_statement* get_aggregate();
  bool is_nested_aggregate(base_statement* e);
  bool is_binop_aggregate_and_column(base_statement* skip);

  virtual void set_last_call()
  {
    is_last_call = true;
    if(left())
    {
      left()->set_last_call();
    }
    if(right())
    {
      right()->set_last_call();
    }
  }

  bool is_set_last_call()
  {
    return is_last_call;
  }

  void invalidate_cache_result()
  {
    m_is_cache_result = false;
  }

  bool is_result_cached()
  {
    return m_is_cache_result == true;
  }

  void set_result_cache(value& eval_result)
  {
    m_alias_result = eval_result;
    m_is_cache_result = true;
  }

  void dec_call_stack_depth()
  {
    m_eval_stack_depth --;
  }

  value& get_result_cache()
  {
    return m_alias_result;
  }

  int& get_eval_call_depth()
  {
    m_eval_stack_depth++;
    return m_eval_stack_depth;
  }

  virtual ~base_statement() {}

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
  char m_star_op_result_charc[4096]; //TODO should be dynamic

  const int undefined_column_pos = -1;
  const int column_alias = -2;

public:
  variable():m_var_type(var_t::NA), _name(""), column_pos(-1) {}

  variable(int64_t i) : m_var_type(var_t::COL_VALUE), column_pos(-1), var_value(i) {}

  variable(double d) : m_var_type(var_t::COL_VALUE), _name("#"), column_pos(-1), var_value(d) {}

  variable(int i) : m_var_type(var_t::COL_VALUE), column_pos(-1), var_value(i) {}

  variable(const std::string& n) : m_var_type(var_t::VAR), _name(n), column_pos(-1) {}

  variable(const std::string& n,  var_t tp) : m_var_type(var_t::NA)
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
      var_value = n.c_str();
    }
    else if (tp ==variable::var_t::STAR_OPERATION)
    {
      _name = "#";
      m_var_type = tp;
      column_pos = -1;
    }
  }

  void operator=(value& v)
  {
    var_value = v;
  }

  void set_value(const char* s)
  {
    var_value = s;
  }

  void set_value(double d)
  {
    var_value = d;
  }

  void set_value(int64_t i)
  {
    var_value = i;
  }

  void set_value(boost::posix_time::ptime* p)
  {
    var_value = p;
  }

  virtual ~variable() {}

  virtual bool is_column()  //is reference to column.
  {
    if(m_var_type == var_t::VAR || m_var_type == var_t::POS)
    {
      return true;
    }
    return false;
  }

  value& get_value()
  {
    return var_value; //TODO is it correct
  }
  virtual value::value_En_t get_value_type()
  {
    return var_value.type;
  }


  value& star_operation()   //purpose return content of all columns in a input stream
  {


    int i;
    size_t pos=0;
    int num_of_columns = m_scratch->get_num_of_columns();
    for(i=0; i<num_of_columns-1; i++)
    {
      size_t len = m_scratch->get_column_value(i).size();
      if((pos+len)>sizeof(m_star_op_result_charc))
      {
        throw base_s3select_exception("result line too long", base_s3select_exception::s3select_exp_en_t::FATAL);
      }

      memcpy(&m_star_op_result_charc[pos], m_scratch->get_column_value(i).data(), len);
      pos += len;
      m_star_op_result_charc[ pos ] = ',';//TODO need for another abstraction (per file type)
      pos ++;

    }

    size_t len = m_scratch->get_column_value(i).size();
    if((pos+len)>sizeof(m_star_op_result_charc))
    {
      throw base_s3select_exception("result line too long", base_s3select_exception::s3select_exp_en_t::FATAL);
    }

    memcpy(&m_star_op_result_charc[pos], m_scratch->get_column_value(i).data(), len);
    m_star_op_result_charc[ pos + len ] = 0;
    var_value = (char*)&m_star_op_result_charc[0];
    return var_value;
  }

  virtual value& eval()
  {
    if (m_var_type == var_t::COL_VALUE)
    {
      return var_value;  // a literal,could be deciml / float / string
    }
    else if(m_var_type == var_t::STAR_OPERATION)
    {
      return star_operation();
    }
    else if (column_pos == undefined_column_pos)
    {
      //done once , for the first time
      column_pos = m_scratch->get_column_pos(_name.c_str());

      if(column_pos>=0 && m_aliases->search_alias(_name.c_str()))
      {
        throw base_s3select_exception(std::string("multiple definition of column {") + _name + "} as schema-column and alias", base_s3select_exception::s3select_exp_en_t::FATAL);
      }


      if (column_pos == undefined_column_pos)
      {
        //not belong to schema , should exist in aliases
        m_projection_alias = m_aliases->search_alias(_name.c_str());

        //not enter this scope again
        column_pos = column_alias;
        if(m_projection_alias == 0)
        {
          throw base_s3select_exception(std::string("alias {")+_name+std::string("} or column not exist in schema"), base_s3select_exception::s3select_exp_en_t::FATAL);
        }
      }

    }

    if (m_projection_alias)
    {
      if (m_projection_alias->get_eval_call_depth()>2)
      {
        throw base_s3select_exception("number of calls exceed maximum size, probably a cyclic reference to alias", base_s3select_exception::s3select_exp_en_t::FATAL);
      }

      if (m_projection_alias->is_result_cached() == false)
      {
        var_value = m_projection_alias->eval();
        m_projection_alias->set_result_cache(var_value);
      }
      else
      {
        var_value = m_projection_alias->get_result_cache();
      }

      m_projection_alias->dec_call_stack_depth();
    }
    else
    {
      var_value = (char*)m_scratch->get_column_value(column_pos).data();  //no allocation. returning pointer of allocated space
    }

    return var_value;
  }

  virtual std::string print(int ident)
  {
    //std::string out = std::string(ident,' ') + std::string("var:") + std::to_string(var_value.__val.num);
    //return out;
    return std::string("#");//TBD
  }

  virtual bool semantic()
  {
    return false;
  }

};

class arithmetic_operand : public base_statement
{

public:

  enum class cmp_t {NA, EQ, LE, LT, GT, GE, NE} ;

private:
  base_statement* l;
  base_statement* r;

  cmp_t _cmp;
  value var_value;

public:

  virtual bool semantic()
  {
    return true;
  }

  virtual base_statement* left()
  {
    return l;
  }
  virtual base_statement* right()
  {
    return r;
  }

  virtual std::string print(int ident)
  {
    //std::string out = std::string(ident,' ') + "compare:" += std::to_string(_cmp) + "\n" + l->print(ident-5) +r->print(ident+5);
    //return out;
    return std::string("#");//TBD
  }

  virtual value& eval()
  {

    switch (_cmp)
    {
    case cmp_t::EQ:
      return var_value = (l->eval() == r->eval());
      break;

    case cmp_t::LE:
      return var_value = (l->eval() <= r->eval());
      break;

    case cmp_t::GE:
      return var_value = (l->eval() >= r->eval());
      break;

    case cmp_t::NE:
      return var_value = (l->eval() != r->eval());
      break;

    case cmp_t::GT:
      return var_value = (l->eval() > r->eval());
      break;

    case cmp_t::LT:
      return var_value = (l->eval() < r->eval());
      break;

    default:
      throw base_s3select_exception("internal error");
      break;
    }
  }

  arithmetic_operand(base_statement* _l, cmp_t c, base_statement* _r):l(_l), r(_r), _cmp(c) {}

  virtual ~arithmetic_operand() {}
};

class logical_operand : public base_statement
{

public:

  enum class oplog_t {AND, OR, NA};

private:
  base_statement* l;
  base_statement* r;

  oplog_t _oplog;
  value var_value;

public:

  virtual base_statement* left()
  {
    return l;
  }
  virtual base_statement* right()
  {
    return r;
  }

  virtual bool semantic()
  {
    return true;
  }

  logical_operand(base_statement* _l, oplog_t _o, base_statement* _r):l(_l), r(_r), _oplog(_o) {}

  virtual ~logical_operand() {}

  virtual std::string print(int ident)
  {
    //std::string out = std::string(ident, ' ') + "logical_operand:" += std::to_string(_oplog) + "\n" + l->print(ident - 5) + r->print(ident + 5);
    //return out;
    return std::string("#");//TBD
  }
  virtual value& eval()
  {
    if (_oplog == oplog_t::AND)
    {
      if (!l || !r)
      {
        throw base_s3select_exception("missing operand for logical and", base_s3select_exception::s3select_exp_en_t::FATAL);
      }
      return var_value =  (l->eval().i64() && r->eval().i64());
    }
    else
    {
      if (!l || !r)
      {
        throw base_s3select_exception("missing operand for logical or", base_s3select_exception::s3select_exp_en_t::FATAL);
      }
      return var_value =  (l->eval().i64() || r->eval().i64());
    }
  }

};

class mulldiv_operation : public base_statement
{

public:

  enum class muldiv_t {NA, MULL, DIV, POW} ;

private:
  base_statement* l;
  base_statement* r;

  muldiv_t _mulldiv;
  value var_value;

public:

  virtual base_statement* left()
  {
    return l;
  }
  virtual base_statement* right()
  {
    return r;
  }

  virtual bool semantic()
  {
    return true;
  }

  virtual std::string print(int ident)
  {
    //std::string out = std::string(ident, ' ') + "mulldiv_operation:" += std::to_string(_mulldiv) + "\n" + l->print(ident - 5) + r->print(ident + 5);
    //return out;
    return std::string("#");//TBD
  }

  virtual value& eval()
  {
    switch (_mulldiv)
    {
    case muldiv_t::MULL:
      return var_value = l->eval() * r->eval();
      break;

    case muldiv_t::DIV:
      return var_value = l->eval() / r->eval();
      break;

    case muldiv_t::POW:
      return var_value = l->eval() ^ r->eval();
      break;

    default:
      throw base_s3select_exception("internal error");
      break;
    }
  }

  mulldiv_operation(base_statement* _l, muldiv_t c, base_statement* _r):l(_l), r(_r), _mulldiv(c) {}

  virtual ~mulldiv_operation() {}
};

class addsub_operation : public base_statement
{

public:

  enum class addsub_op_t {ADD, SUB, NA};

private:
  base_statement* l;
  base_statement* r;

  addsub_op_t _op;
  value var_value;

public:

  virtual base_statement* left()
  {
    return l;
  }
  virtual base_statement* right()
  {
    return r;
  }

  virtual bool semantic()
  {
    return true;
  }

  addsub_operation(base_statement* _l, addsub_op_t _o, base_statement* _r):l(_l), r(_r), _op(_o) {}

  virtual ~addsub_operation() {}

  virtual std::string print(int ident)
  {
    //std::string out = std::string(ident, ' ') + "addsub_operation:" += std::to_string(_op) + "\n" + l->print(ident - 5) + r->print(ident + 5);
    return std::string("#");//TBD
  }

  virtual value& eval()
  {
    if (_op == addsub_op_t::NA) // -num , +num , unary-operation on number
    {
      if (l)
      {
        return var_value = l->eval();
      }
      else if (r)
      {
        return var_value = r->eval();
      }
    }
    else if (_op == addsub_op_t::ADD)
    {
      return var_value = (l->eval() + r->eval());
    }
    else
    {
      return var_value = (l->eval() - r->eval());
    }

    return var_value;
  }
};

class base_function
{

protected:
  bool aggregate;

public:
  //TODO add semantic to base-function , it operate once on function creation
  // validate semantic on creation instead on run-time
  virtual bool operator()(bs_stmt_vec_t* args, variable* result) = 0;
  base_function() : aggregate(false) {}
  bool is_aggregate()
  {
    return aggregate == true;
  }
  virtual void get_aggregate_result(variable*) {}

  virtual ~base_function() {}
  
  virtual void __call_destructor()
  {
    this->~base_function();
  }
};


};//namespace

#endif
