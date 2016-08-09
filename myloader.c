/* 
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

    Authors:        Andrew Hutchings, SkySQL (andrew at skysql dot com)
*/

#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

#include <mysql.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <zlib.h>
#include "common.h"
#include "myloader.h"
#include "config.h"

guint commit_count= 1000;
gchar *directory= NULL;
gchar *inputfile= NULL;
gboolean overwrite_tables= FALSE;
gboolean truncate_tables= FALSE;
gboolean ignore_indexes=FALSE;
gboolean enable_binlog= FALSE;
gchar *source_db= NULL;
guint innodb_buffer_pool_size=0;
guint count_in_files=1000;
gboolean dry_run = FALSE;
gboolean report = FALSE;

static GMutex *init_mutex= NULL;
static GMutex *db_mutex=NULL;

guint errors= 0;
void db_feeder( struct configuration *conf);
void read_file_process( struct configuration *conf);
gboolean read_data(FILE *file, gboolean is_compressed, GString *data, gboolean *eof);
gboolean read_line(FILE *file, gboolean is_compressed, GString *data, gboolean *eof);
void restore_data(MYSQL *conn, char *database, char *table, const char *filename, gboolean is_schema, gboolean need_use);
void *process_queue(struct thread_data *td);
void add_schema(const gchar* filename, MYSQL *conn);
void add_schema_string(gchar* database, gchar *table, GString* statement, MYSQL *conn);
void restore_databases(struct configuration *conf, MYSQL *conn);
void restore_schema_view(MYSQL *conn);
void restore_schema_triggers(MYSQL *conn);
void restore_schema_post(MYSQL *conn);
void restore_schema_constraints(struct table_data *table, MYSQL *conn);
void get_database_table(gchar *filename, gchar **database, gchar **table);
int restore_string(MYSQL *conn, char *database, char *table, GString *data, gboolean is_schema);
void no_log(const gchar *log_domain, GLogLevelFlags log_level, const gchar *message, gpointer user_data);
void set_verbose(guint verbosity);
void create_database(MYSQL *conn, gchar *database);
void order_files(MYSQL *conn, struct configuration *conf);
void add_index(struct table_data * td, struct configuration *conf);
void show_report(GSList *table_data_list, GSList *schema_data_list);
void *monitor_process(struct thread_data *td);
void get_database(gchar *filename, gchar **database);
void read_database_table (char * split_dbname_tablename, char **database, char **table);
struct datafiles * new_datafile_filename(const char* filename);
void parsing_create_statement(char *data, struct table_data *td, struct configuration *conf);
void add_job( GAsyncQueue* queue, char * database, char * table, struct datafiles *df , enum job_type jt);
void add_message_job( GAsyncQueue* queue, const char * message);

static GOptionEntry entries[] =
{
	{ "directory", 'd', 0, G_OPTION_ARG_STRING, &directory, "Directory of the dump to import", NULL },
	{ "queries-per-transaction", 'q', 0, G_OPTION_ARG_INT, &commit_count, "Number of queries per transaction, default 1000", NULL },
	{ "overwrite-tables", 'o', 0, G_OPTION_ARG_NONE, &overwrite_tables, "Drop tables if they already exist", NULL },
        { "truncate-tables", 't', 0, G_OPTION_ARG_NONE, &truncate_tables, "Truncate tables that on the server", NULL },
        { "ignore-indexes", 'x', 0, G_OPTION_ARG_NONE, &ignore_indexes, "It will ignore the creation of the indexes at the end", NULL },
	{ "database", 'B', 0, G_OPTION_ARG_STRING, &db, "An alternative database to restore into", NULL },
	{ "source-db", 's', 0, G_OPTION_ARG_STRING, &source_db, "Database to restore", NULL },
	{ "enable-binlog", 'e', 0, G_OPTION_ARG_NONE, &enable_binlog, "Enable binary logging of the restore data", NULL },
	{ "innodb-buffer-pool-size", 's', 0, G_OPTION_ARG_INT, &innodb_buffer_pool_size, "The innodb buffer pool size which limits the order of the tables", NULL },
        { "count", 'c', 0, G_OPTION_ARG_INT, &count_in_files, "The amount of lines per file, to avoid to count them", NULL },
        { "dry-run", 'r', 0, G_OPTION_ARG_NONE, &dry_run, "Dry run, it will not change the database just will print the order of the tasks", NULL },
        { "report", 'z', 0, G_OPTION_ARG_NONE, &report, "Report the table order", NULL },
        { "file", 'f', 0, G_OPTION_ARG_STRING, &inputfile, "File of the dump to import", NULL },
	{ NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL }
};

void no_log(const gchar *log_domain, GLogLevelFlags log_level, const gchar *message, gpointer user_data) {
	(void) log_domain;
	(void) log_level;
	(void) message;
	(void) user_data;
}

void set_verbose(guint verbosity) {
	switch (verbosity) {
		case 0:
			g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MASK), no_log, NULL);
			break;
		case 1:
			g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_WARNING | G_LOG_LEVEL_MESSAGE), no_log, NULL);
			break;
		case 2:
			g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MESSAGE), no_log, NULL);
			break;
		default:
			break;
	}
}

void print_table(struct table_data *table) {
	g_message("%s.%s\t%llu\t%s\t%s\t%u", table->database, table->table,table->size,table->indexes ? "YES" : "NO",table->constraints ? "YES" : "NO",g_slist_length(table->datafiles_list) );
}


void check_df_status (struct datafiles *df, gboolean *b) {
	*b=*b && f_TERMINATED == df->status;
}

//enum table_state { t_NOT_CREATED, t_CREATING, t_CREATED, t_RUNNING_DATA, t_DATA_TERMINATED, t_RUNNING_INDEXES, t_WAITING, t_TERMINATED };
void check_status (struct table_data *table, gboolean *b) {
	g_slist_foreach(table->datafiles_list, (GFunc)check_df_status, b);
	if (table->schema)
		*b=*b && f_TERMINATED == table->schema->status;
}

void df_not_ended (struct datafiles *df, gint * amount) {
        if (df->status != f_TERMINATED) (*amount)++;
}

void check_db_status (struct table_data *table){
	if (table->status == t_RUNNING_DATA){
		gint amount;
		amount=0;
		g_slist_foreach(table->datafiles_list, (GFunc)df_not_ended, &amount);
		if (amount == 0 )
			table->status = t_TERMINATED;
	}
}

void df_undone (struct datafiles *df, gint * amount) {
        if (df->status == f_CREATED) { (*amount)++;
//		 g_message("Table status %d  amount %d",df->status,*amount);
		}
}

void table_undone(struct table_data *table, gint * amount){
	if (table->status == t_RUNNING_DATA) 
	        g_slist_foreach(table->datafiles_list, (GFunc)df_undone, amount);
// enum table_state { t_NOT_CREATED, t_CREATING, t_CREATED, t_RUNNING_DATA, t_DATA_TERMINATED, t_RUNNING_INDEXES, t_WAITING, t_TERMINATED };
	if (table->status != t_TERMINATED)
		(*amount)++;
//	if (table->indexes) (*amount)++;
}


void print_schema(struct schema_data *schema) {
        g_message("%s\t%s\t%s\t%s\t%s", schema->database,schema->view_file ? schema->view_file->filename:"",schema->trigger_file ? schema->trigger_file->filename: "",schema->post_file ? schema->post_file->filename : "",schema->create_schema_file ? schema->create_schema_file->filename:"");
}

int main(int argc, char *argv[]) {
	struct configuration conf= { NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0 };

	GError *error= NULL;
	GOptionContext *context;

	g_thread_init(NULL);

	init_mutex= g_mutex_new();
	db_mutex=g_mutex_new();

	if(db == NULL && source_db != NULL){
		db = g_strdup(source_db);
	}

	context= g_option_context_new("multi-threaded MySQL loader");
	GOptionGroup *main_group= g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
	g_option_group_add_entries(main_group, entries);
	g_option_group_add_entries(main_group, common_entries);
	g_option_context_set_main_group(context, main_group);
	if (!g_option_context_parse(context, &argc, &argv, &error)) {
		g_print("option parsing failed: %s, try --help\n", error->message);
		exit(EXIT_FAILURE);
	}
	g_option_context_free(context);

	if (program_version) {
		g_print("myloader %s, built against MySQL %s\n", VERSION, MYSQL_SERVER_VERSION);
		exit(EXIT_SUCCESS);
	}

	set_verbose(verbose);
	if (inputfile) {
		if (directory) {
                        g_critical("File and directory options are incompatible, see --help\n");
                        exit(EXIT_FAILURE);
                } else {
                        if (!g_file_test(inputfile, G_FILE_TEST_EXISTS)) {
                                g_critical("the specified input file doesn't exists\n");
                                exit(EXIT_FAILURE);
                        }
                }
	}else{
		if (!directory) {
			g_critical("a directory needs to be specified, see --help\n");
			exit(EXIT_FAILURE);
		} else {
			char *p= g_strdup_printf("%s/metadata", directory);
			if (!g_file_test(p, G_FILE_TEST_EXISTS)) {
				g_critical("the specified directory is not a mydumper backup\n");
				exit(EXIT_FAILURE);
			}
		}
	}
	MYSQL *conn;
	conn= mysql_init(NULL);
	mysql_options(conn, MYSQL_READ_DEFAULT_GROUP, "myloader");

	if (!mysql_real_connect(conn, hostname, username, password, NULL, port, socket_path, 0)) {
		g_critical("Error connection to database: %s", mysql_error(conn));
		exit(EXIT_FAILURE);
	}

	if (mysql_query(conn, "SET SESSION wait_timeout = 2147483")){
		g_warning("Failed to increase wait_timeout: %s", mysql_error(conn));
	}

	if (!enable_binlog)
		mysql_query(conn, "SET SQL_LOG_BIN=0");

	mysql_query(conn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");
	conf.queue= g_async_queue_new();
	conf.ready= g_async_queue_new();
	conf.rqueue= g_async_queue_new();
	conf.squeue= g_async_queue_new();
	guint n;

	GThread **threads= g_new(GThread*, num_threads+3);
	struct thread_data *td= g_new(struct thread_data, num_threads);
	for (n= 0; n < num_threads; n++) {
		td[n].conf= &conf;
		td[n].thread_id= n+1;
		threads[n]= g_thread_create((GThreadFunc)process_queue, &td[n], TRUE, NULL);
		add_message_job(conf.rqueue,"STARTING");
		g_async_queue_pop(conf.ready);
	}
	g_async_queue_unref(conf.ready);

	g_message("%d threads created", num_threads);
	if (inputfile) {
		g_message("Creating process");
		threads[num_threads+1]= g_thread_create((GThreadFunc)read_file_process, &conf, TRUE, NULL);
		threads[num_threads+2]= g_thread_create((GThreadFunc)db_feeder, &conf, TRUE, NULL);
	}else{
		// I'm going to order the tables by the sum of all the file size
		order_files(conn,&conf);

		// Print tables
		//g_slist_foreach(conf.ordered_tables, (GFunc)print_table, NULL);
		show_report(conf.ordered_tables,conf.schema_data_list);
	}
	struct thread_data *tm= g_new(struct thread_data, 1);
	tm[0].conf= &conf;
	tm[0].thread_id= 1;
	// Start the Monitor Process
	threads[num_threads]=g_thread_create((GThreadFunc)monitor_process, &tm[0], TRUE, NULL);

	for (n= 0; n < num_threads; n++) {
		g_thread_join(threads[n]);
	}


        // This will end the Monitor Process
	add_message_job(conf.rqueue,"MYLOADER-ENDITUP");

        for (n= num_threads; n < num_threads+3; n++) {
                g_thread_join(threads[n]);
        }
	my_bool  my_true = TRUE;
	mysql_options(conn, MYSQL_OPT_RECONNECT, &my_true);
	mysql_query(conn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");
	if (!enable_binlog)
		mysql_query(conn, "SET SQL_LOG_BIN=0");

	if (!mysql_ping(conn))
		g_slist_foreach(conf.constraint_list, (GFunc)restore_schema_constraints, conn);
	else
		g_critical("Error creating constraints:%s", mysql_error(conn));

	if (!inputfile){
		restore_schema_post(conn);

		restore_schema_view(conn);

		restore_schema_triggers(conn);
	}
	g_async_queue_unref(conf.queue);
	g_async_queue_unref(conf.rqueue);
	g_async_queue_unref(conf.squeue);
	mysql_close(conn);
	mysql_thread_end();
	mysql_library_end();
	if (directory)
		g_free(directory);
	g_free(td);
	g_free(tm);
	g_free(threads);

	g_mutex_free(init_mutex);
	g_mutex_free(db_mutex);

	return errors ? EXIT_FAILURE : EXIT_SUCCESS;
}



gint compare_datafiles(gconstpointer a, gconstpointer b){
	const struct datafiles *ab = a;
	const struct datafiles *bb = b;
	return strcmp(ab->filename, bb->filename);
}

struct datafiles *get_filename(GSList *datafiles_list, gchar* filename){
	struct datafiles td;
	GSList * list;
	td.filename=filename;
	list=g_slist_find_custom(datafiles_list,&td, (GCompareFunc)compare_datafiles);
	if (list)
		return list->data;
	return NULL;
}

gint compare_schemas(gconstpointer a, gconstpointer b){
        const struct schema_data *ab = a;
        const struct schema_data *bb = b;
        return (strcmp(ab->database, bb->database)==0);
}


gint uncompleted_tables(gconstpointer a, gconstpointer b){
        const struct table_data *ab = a;
	const struct table_data *bb = b;
        return (ab->status == t_TERMINATED) - (bb->status == t_TERMINATED);
}

gint compare_tables(gconstpointer a, gconstpointer b){
	const struct table_data *ab = a;
	const struct table_data *bb = b;
	return !((strcmp(ab->table, bb->table)==0) && (strcmp(ab->database, bb->database)==0));
}

gint compare_table_size(gconstpointer a, gconstpointer b){
	const struct table_data *ab = a;
	const struct table_data *bb = b;
	if (bb->size > ab->size){
		if (ab->size > innodb_buffer_pool_size){
			return 1;
		}else{
			if (innodb_buffer_pool_size > bb->size){
				return -1;
			}else{
				return 1;
			}
		}
	}
	if (bb->size < ab->size){
		if (innodb_buffer_pool_size < bb->size){
			return -1;
		}else{
			if (ab->size < innodb_buffer_pool_size){
				return -1;
			}else{
				return 1;
			}
		}
	}
	return 0;
}

struct table_data *get_table(GSList *table_data_list, gchar* database, gchar* table){
	struct table_data td;
	if (database == NULL)
		td.database=strdup(db);
	else
		td.database=strdup(database);
	td.table=strdup(table);
	GSList * list = g_slist_find_custom(table_data_list,&td, (GCompareFunc)compare_tables);
	g_free(td.table);
	g_free(td.database);
	if (list){
		return list->data;
	}
	return NULL;
}

struct schema_data * new_schema (gchar* database){
	struct schema_data *sd=g_new0(struct schema_data,1);
	sd->database=database;
	sd->view_file=NULL;
	sd->trigger_file=NULL;
	sd->post_file=NULL;
	sd->create_schema_file=NULL;
	return sd;
}

struct schema_data *get_schema(struct configuration *conf, gchar* database){
	GSList *schema_data_list=conf->schema_data_list;
	struct schema_data st;
	g_critical("cannot open   %s",database);
	st.database=strdup(database);
	GSList * list = g_slist_find_custom(schema_data_list,&st, (GCompareFunc)compare_schemas);
	if (list){
		return list->data;
	}
	struct schema_data * nst=new_schema(database);
	conf->schema_data_list = g_slist_append (schema_data_list,nst);
	return nst;
}

struct datafiles * new_datafile(){
	struct datafiles *df=g_new0(struct datafiles,1);
	df->status=f_CREATED;
	df->dml_statement=NULL;
	df->filename=NULL;
	df->ddl_statement=NULL;
	return df;
}

struct datafiles * new_datafile_filename(const char* filename){
        struct datafiles *df=new_datafile();
        df->filename=g_strdup(filename);
        gchar** split_file= g_strsplit(filename, ".", 0);
        df->part= g_ascii_strtoull(split_file[2], NULL, 10);
	g_strfreev(split_file);
        return df;
}

struct datafiles * new_datafile_ddl_statement(GString *statement){
        struct datafiles *df=new_datafile();
        df->ddl_statement=statement;
        return df;
}

struct datafiles * new_datafile_dml_statement(GString *statement){
        struct datafiles *df=new_datafile();
        df->dml_statement=statement;
        return df;
}

struct table_data* new_table(gchar* database,gchar* table){
	struct table_data *td=g_new0(struct table_data,1);
	td->datafiles_list=NULL;
	if (database == NULL)
		td->database=strdup(db);
	else
		td->database=strdup(database);
	td->table=strdup(table);
	td->schema=NULL;
	td->indexes=NULL;
	td->constraints=NULL;
	td->schemafile=NULL;
	td->status=t_NOT_CREATED;
	td->size=td->size*0;
	td->amount_of_rows=0;
return td;
}

struct table_data * add_file_to_list(GSList **table_data_list, char* filename){
	struct table_data *td;
	
	if (g_strrstr(filename, ".sql")){
		
		gchar *database,*table= NULL;
		get_database_table(filename,&database,&table);
		td=get_table(*table_data_list,database,table);

		if (td==NULL ){
			td=new_table(database,table);
			*table_data_list = g_slist_append (*table_data_list,td);
		}

		if (g_strrstr(filename, "-schema.sql")) {
			struct datafiles *df=new_datafile_filename(filename);
			td->schemafile=df;
		}else
		if (g_strrstr(filename, ".sql")
//		   && !g_strrstr(filename, "-schema-view.sql")
//		   && !g_strrstr(filename, "-schema-triggers.sql")
//		   && !g_strrstr(filename, "-schema-post.sql")
//		   && !g_strrstr(filename, "-schema-create.sql")
		){
			struct datafiles *df=new_datafile_filename(filename);
			td->datafiles_list=g_slist_append(td->datafiles_list,df);
			char * pp=g_strdup(g_strconcat(directory,"/",filename,NULL));
			GMappedFile *mapping = g_mapped_file_new (pp, FALSE, NULL);
			td->size += g_mapped_file_get_length (mapping);
		}
		return td;
	}else{
			g_message("File avoided: %s", filename);
	}
	return NULL;
}

void show_report(GSList *table_data_list, GSList *schema_data_list){
//	g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MESSAGE), no_log, NULL);
	g_message("Report");
	g_slist_foreach(schema_data_list, (GFunc)print_schema, NULL);
	g_slist_foreach(table_data_list, (GFunc)print_table, NULL);
//	set_verbose(verbose);
}

gboolean push_next_job(struct configuration * conf){
	GSList * ot=conf->ordered_tables;
//	g_message("Push next job");
	while ( ot ){
		int amountrunning=0;
		struct table_data * table =ot->data;
		struct datafiles *df=NULL;
		GSList *elem=NULL;
		gboolean b=TRUE;
		switch (table->status){
			case t_NOT_CREATED:
				if (table->schema){
				        g_message("Creando TAbla");
					table->status=t_CREATING;
					table->schema->status=f_RUNNING;
					add_job( conf->queue, table->database, table->table, table->schema , JOB_SCHEMA);
				}else{
					if (truncate_tables){
						
					}
					
					g_message("No schema yet for %s.%s",table->database, table->table);
				}
				return TRUE;
				break;
			case t_CREATED:
                                table->status=t_RUNNING_DATA;
                                break;
			case t_CREATING:
				ot=g_slist_next(ot);
				break;
			case t_RUNNING_DATA:
				check_status(table,&b);
				if (b){
					table->status=t_DATA_TERMINATED;
				}else{
					elem=table->datafiles_list;
                        		if (elem)
                	                	df=elem->data;
			                        while (elem && df->status != f_CREATED ){
	                		                elem=g_slist_next(elem);
                                			if (df->status == f_RUNNING)
		                        	                amountrunning+=1;
                			                if (elem)
                        	        		        df = elem->data;
			                        }
					if (df && df->status == f_CREATED){
						df->status=f_RUNNING;
						add_job( conf->queue, table->database, table->table, df , JOB_RESTORE);
						return TRUE;
					}else{
						ot=g_slist_next(ot);
					}
				}
                                break;
			case t_DATA_TERMINATED:
//				g_message("* Data processed *");
                                table->status=t_RUNNING_INDEXES;
                                if (table->indexes ){
					if (!ignore_indexes){
//						g_message("* Running indexes *");
						add_job( conf->queue, table->database, table->table, table->indexes , JOB_INDEX);
		                                table->status=t_WAITING;
						return TRUE;
					}else{
//						g_message("* Ignoring indexes *");
						table->status=t_TERMINATED;
					}
				}else{
					table->status=t_TERMINATED;
				}
				break;
			case t_RUNNING_INDEXES:
				ot=g_slist_next(ot);
				break;
			case t_WAITING:
				ot=g_slist_next(ot);
                                break;
			case t_TERMINATED:
                                ot=g_slist_next(ot);
                                break;
		}
	}
	return FALSE;
}

void order_files(MYSQL *conn, struct configuration *conf) {
	GSList *table_data_list=NULL;
	GError *error= NULL;
	GDir* dir= g_dir_open(directory, 0, &error);
	if (error) {
		g_critical("cannot open directory %s, %s\n", directory, error->message);
		errors++;
		return;
	}
	const gchar* filename= NULL;
	while((filename= g_dir_read_name(dir))) {
		if (!source_db || g_str_has_prefix(filename, g_strdup_printf("%s.", source_db))){
			gchar* database=NULL;
			get_database((gchar *)filename,&database);
			if (g_strrstr(filename, "-schema-view.sql")){
				get_schema(conf,database)->view_file=new_datafile_filename(filename);
				g_critical("cannot open   %s",database);
				continue;
			}
                   	if (g_strrstr(filename, "-schema-triggers.sql")){
				get_schema(conf,database)->trigger_file=new_datafile_filename(filename);
				continue;
			}
			if (g_strrstr(filename, "-schema-post.sql")){
				get_schema(conf,database)->post_file=new_datafile_filename(filename);
				continue;
			}
			if (g_strrstr(filename, "-schema-create.sql")){
				get_schema(conf,database)->create_schema_file=new_datafile_filename(filename);
				continue;
			}
			struct table_data * td=add_file_to_list(&table_data_list,(gchar* )filename);
			if (td && g_strrstr(filename, "-schema.sql")) {
				/* This code has to parse the data in the schema file.
				 * This code has to split the create table statement without indexes and constraints.
				 * This code has to create the indexes and the constraints statements
				 * The create table statement is executed now
				 * The indexes are created after all data files are imported
				 * The constraints are created at the end.
				 */

				gchar* table= NULL;
				get_database_table((gchar *)filename,&database,&table);
			
				td->schemafile=new_datafile_filename(filename);
			
				gboolean eof= FALSE;
				void *infile;
				gboolean is_compressed= FALSE;
				GString *data= g_string_sized_new(512);
				gchar* path= g_build_filename(directory, filename, NULL);

				if (!g_str_has_suffix(path, ".gz")) {
					infile= g_fopen(path, "r");
					is_compressed= FALSE;
				} else {
					infile= (void*) gzopen(path, "r");
					is_compressed= TRUE;
				}

				// Read the content of the schema file
				while (eof == FALSE) {
					read_data(infile, is_compressed, data, &eof);
				}

				if (!is_compressed) {
					fclose(infile);
				} else {
					gzclose((gzFile)infile);
				}

				parsing_create_statement(data->str, td, conf);
/*
				// Will read the file line by line
				gchar** split_file= g_strsplit(data->str, "\n", -1);

				int i,commafix=0;
			
				// Parsing the lines
				for (i=0; i < (int)g_strv_length(split_file);i++){
					if (   g_strrstr(split_file[i],"  KEY")
							|| g_strrstr(split_file[i],"  UNIQUE")
							|| g_strrstr(split_file[i],"  SPATIAL")
							|| g_strrstr(split_file[i],"  FULLTEXT")
							|| g_strrstr(split_file[i],"  INDEX")
						){
						// This line is a index 						
						commafix=1;
						gchar *poschar=g_strrstr(split_file[i],",");
						if (poschar && *(poschar+1)=='\0')
							*poschar='\0';

						if (td->indexes == NULL){
*/ //							td->indexes=new_datafile_ddl_statement(g_string_new ("/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n\nALTER TABLE `"));
/*							GString *ni = td->indexes->ddl_statement;	
							ni=g_string_append(ni, table);
							ni=g_string_append(ni, "` ");
						}else{
							GString *ni = td->indexes->ddl_statement;
							ni=g_string_append(ni, ",");
						}
						GString *ni = td->indexes->ddl_statement;
                                                ni= g_string_append(g_string_append(ni,"\n  ADD "), split_file[i]);
					}else
					if ( g_strrstr(split_file[i],"  CONSTRAINT") ){
						// This line is a constraint 
					
						commafix=1;
						gchar *poschar=g_strrstr(split_file[i],",");
						if (poschar && *(poschar+1)=='\0')
							*poschar='\0';
						if (td->constraints == NULL ){
*/ //							td->constraints=new_datafile_ddl_statement(g_string_new ("/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n\nALTER TABLE `"));
/*							GString *ni = td->constraints->ddl_statement;
							ni=g_string_append(ni, table);
							ni=g_string_append(ni, "` ");
						}else{
							GString *ni = td->constraints->ddl_statement;
							ni=g_string_append(ni, ",");
						}
						GString *ni = td->constraints->ddl_statement;
						ni=g_string_append(g_string_append(ni,"\n  ADD "), split_file[i]);
					}else{
						// This line is a piece of the create table statement
						if (commafix){
							commafix=0;
							gchar *poschar=g_strrstr(td->schema->ddl_statement->str,",");
							if (poschar && *(poschar+1)=='\0')
								*poschar=' ';
						}
						GString *ni = td->schema->ddl_statement;
						ni=g_string_append(g_string_append(ni,"\n"), split_file[i]);
					}
				}
				g_strfreev(split_file);
				// Set create table statement to the table
				add_schema_string(database,table,td->schema->ddl_statement,conn);
			
				// Set the constraints to the table
				if (td->constraints != NULL){
					g_message("Spliting constraint for `%s`.`%s`", db ? db : database, table);
					td->constraints->ddl_statement=g_string_append(td->constraints->ddl_statement,";\n");
					conf->constraint_list = g_slist_append (conf->constraint_list,td);
				}
			
				// Set the indexes to the table
				if (td->indexes != NULL){
					td->indexes->ddl_statement=g_string_append(td->indexes->ddl_statement,";\n");
					if (g_slist_length(td->datafiles_list) * count_in_files > 31000){
						g_message("Spliting indexes for `%s`.`%s`", db ? db : database, table);
					}else{
						g_message("Restoring indexes for `%s`.`%s`", db ? db : database, table);
						restore_string(conn, database, table, td->indexes->ddl_statement, TRUE);
						td->indexes=NULL;
					}
				}
*/
			// ADDED after the change
				add_schema_string(database,table,td->schema->ddl_statement,conn);
	                        if (td->indexes != NULL){
                                        if (g_slist_length(td->datafiles_list) * count_in_files > 31000){
                                                g_message("Spliting indexes for `%s`.`%s`", db ? db : database, table);
                                        }else{
                                                g_message("Restoring indexes for `%s`.`%s`", db ? db : database, table);
                                                restore_string(conn, database, table, td->indexes->ddl_statement, TRUE);
                                                td->indexes=NULL;
                                        }
                                }		





			}
		}
	}
	g_dir_close(dir);
//table->datafiles_list	
	// Order the table by size
	conf->ordered_tables=g_slist_sort(table_data_list,compare_table_size);
}

void restore_schema_constraints(struct table_data *table, MYSQL *conn) {
        g_message("Adding constraint for `%s`.`%s`", table->database,table->table);
        restore_string(conn, table->database,table->table, table->constraints->ddl_statement, TRUE);
}

void restore_schema_view(MYSQL *conn){
	GError *error= NULL;
	GDir* dir= g_dir_open(directory, 0, &error);

	if (error) {
		g_critical("cannot open directory %s, %s\n", directory, error->message);
		errors++;
		return;
	}

	const gchar* filename= NULL;

	while((filename= g_dir_read_name(dir))) {
		if (!source_db || g_str_has_prefix(filename, source_db)){
			if (g_strrstr(filename, "-schema-view.sql")) {
				add_schema(filename, conn);
			}
		}
	}

	g_dir_close(dir);
}

void restore_schema_triggers(MYSQL *conn){
	GError *error= NULL;
	GDir* dir= g_dir_open(directory, 0, &error);
	gchar** split_file= NULL;
	gchar* database=NULL;
	gchar* table= NULL;

	if (error) {
		g_critical("cannot open directory %s, %s\n", directory, error->message);
		errors++;
		return;
	}

	const gchar* filename= NULL;

	while((filename= g_dir_read_name(dir))) {
		if (!source_db || g_str_has_prefix(filename, source_db)){
			if (g_strrstr(filename, "-schema-triggers.sql")) {
				gchar** split_table= NULL;
				split_file= g_strsplit(filename, ".", 0);
				database= strdup(split_file[0]);
				split_table= g_strsplit(split_file[1], "-schema", 0);
				table= strdup(split_table[0]);
				g_strfreev(split_table);
				g_message("Restoring triggers for `%s`.`%s`", db ? db : database, table);
				restore_data(conn, database, table, filename, TRUE, TRUE);
			}
		}
	}

	g_strfreev(split_file);
	g_dir_close(dir);
}

void restore_schema_post(MYSQL *conn){
	GError *error= NULL;
	GDir* dir= g_dir_open(directory, 0, &error);
	gchar** split_file= NULL;
	gchar* database=NULL;
	//gchar* table=NULL;


	if (error) {
		g_critical("cannot open directory %s, %s\n", directory, error->message);
		errors++;
		return;
	}

	const gchar* filename= NULL;

	while((filename= g_dir_read_name(dir))) {
		if (!source_db || g_str_has_prefix(filename, source_db)){
			if (g_strrstr(filename, "-schema-post.sql")) {
				split_file= g_strsplit(filename, "-schema-post.sql", 0);
				database= strdup(split_file[0]);
				//table= split_file[0]; //NULL
				g_message("Restoring routines and events for `%s`", db ? db : database);
				restore_data(conn, database, NULL, filename, TRUE, TRUE);
			}
		}
	}

	g_strfreev(split_file);
	g_dir_close(dir);
}

//void create_database(MYSQL *conn, gchar *database){
//
//	gchar* query = NULL;
//
//	query= g_strdup_printf("SHOW CREATE DATABASE `%s`", db ? db : database);
//	if (mysql_query(conn, query)) {
//		g_free(query);
//		g_message("Creating database `%s`", db ? db : database);
//		query= g_strdup_printf("CREATE DATABASE `%s`", db ? db : database);
//               mysql_query(conn, query);
//	} else {
//		MYSQL_RES *result= mysql_store_result(conn);
//		// In drizzle the query succeeds with no rows
//		my_ulonglong row_count= mysql_num_rows(result);
//		mysql_free_result(result);
//		if (row_count == 0) {
//			// TODO: Move this to a function, it is the same as above
//			g_free(query);
//			g_message("Creating database `%s`", db ? db : database);
//			query= g_strdup_printf("CREATE DATABASE `%s`", db ? db : database);
//			mysql_query(conn, query);
//		}
//	}
//	g_free(query);
//	return;
//}

void create_database(MYSQL *conn, gchar *database){

	gchar* query = NULL;

	if((db == NULL && source_db == NULL) || (db != NULL && source_db != NULL && !g_ascii_strcasecmp(db, source_db))){
		const gchar* filename= g_strdup_printf("%s-schema-create.sql", db ? db : database);
		const gchar* filenamegz= g_strdup_printf("%s-schema-create.sql.gz", db ? db : database);

		if (g_file_test (filename, G_FILE_TEST_EXISTS)){
			restore_data(conn, database, NULL, filename, TRUE, FALSE);
		}else if (g_file_test (filenamegz, G_FILE_TEST_EXISTS)){
			restore_data(conn, database, NULL, filenamegz, TRUE, FALSE);
		}else{
			query= g_strdup_printf("CREATE DATABASE `%s`", db ? db : database);
			mysql_query(conn, query);
		}
	}else{
		query= g_strdup_printf("CREATE DATABASE `%s`", db ? db : database);
		mysql_query(conn, query);
	}

	g_free(query);
	return;
}

// I don't use this function, it has to be deleted.
// I use add_schema_string which takes a string instead of a filename
void add_schema(const gchar* filename, MYSQL *conn) {
	gchar* database,*table= NULL;
	get_database_table((gchar *)filename,&database,&table);
	create_database(conn, database);
	if (overwrite_tables) {
		gchar* query;
		g_message("Dropping table (if exists) `%s`.`%s`", db ? db : database, table);
		query= g_strdup_printf("DROP TABLE IF EXISTS `%s`.`%s`", db ? db : database, table);
		mysql_query(conn, query);
		g_free(query);
	}
	g_message("Creating table `%s`.`%s`", db ? db : database, table);
	restore_data(conn, database, table, filename, TRUE,TRUE);
	return;
}

void add_schema_string(gchar* database, gchar *table, GString* statement, MYSQL *conn) {
	create_database(conn, database);
	if (truncate_tables){
                gchar* query;
                g_message("Truncating table (if exists) `%s`.`%s`", db ? db : database, table);
                if (! dry_run){
                        query= g_strdup_printf("TRUNCATE TABLE `%s`.`%s`", db ? db : database, table);
                        mysql_query(conn, query);
                        g_free(query);
                }
	}else{	
		if (overwrite_tables) {
			gchar* query;
			g_message("Dropping table (if exists) `%s`.`%s`", db ? db : database, table);
			if (! dry_run){
				query= g_strdup_printf("DROP TABLE IF EXISTS `%s`.`%s`", db ? db : database, table);
				mysql_query(conn, query);
				g_free(query);
			}
		}	
		g_message("Creating table `%s`.`%s`", db ? db : database, table);
		restore_string(conn, database, table, statement, TRUE);
	}
	return;
}

void get_database(gchar *filename, gchar **database){
	// 0 is database, 1 is table with -schema on the end
	gchar** split_file= g_strsplit(filename, "-", 0);
	gchar* ldb= strdup(split_file[0]);
	*database=strdup(ldb);
	g_critical("asdads %s",*database);
	g_strfreev(split_file);
}

void get_database_table(gchar *filename, gchar **database, gchar **table){
	// 0 is database, 1 is table with -schema on the end
	gchar** split_file= g_strsplit(filename, ".", 0);
	gchar* ldb= strdup(split_file[0]);
	// Remove the -schema from the table name
	gchar** split_table= g_strsplit(split_file[1], "-", 0);
	gchar* tb= strdup(split_table[0]);
	*database=strdup(ldb);
	*table=strdup(tb);
	g_strfreev(split_table);
	g_strfreev(split_file);

}


void destroy_datafile (struct datafiles *df){
	if (df->ddl_statement)
		g_string_free(df->ddl_statement,TRUE);
        if (df->dml_statement)
		g_string_free(df->dml_statement,TRUE);
	g_free(df->filename);
	df->ddl_statement=NULL;
	df->dml_statement=NULL;
	df->filename=NULL;
}

void destroy_restore_job (struct restore_job * rj){
	g_free(rj->table);
	g_free(rj->database);
	destroy_datafile(rj->datafile);
//	g_free(rj->datafile);
	rj->table=NULL;
	rj->database=NULL;
	rj->datafile=NULL;
}

void destroy_job (struct job ** job){
        switch ((*job)->type){
                case JOB_SCHEMA:
                case JOB_INDEX:
                case JOB_RESTORE:
			destroy_restore_job((struct restore_job *)((*job)->job_data));
			g_free((struct restore_job *)((*job)->job_data));
                	break;
		default: break;
	}
	g_free(*job);
}

/*
 * This thread manages which file has to be processed and register the ones
 * that has already been processed. It keeps record of wich table has not 
 * finish yet.
 * It uses a response queue (rqueue) to start and then create the job
 * which is pushed in the queue.
 */ 

void *monitor_process(struct thread_data *td) {
	gboolean canIfinish=FALSE;
	gboolean reader_stopped=FALSE;
	struct configuration * conf=td->conf;
	GSList **ordered_tables=&(conf->ordered_tables);
	const char *cc;
	g_message("Monitor Process Started");
	for(;;){
		struct job * job = (struct job *) g_async_queue_pop(conf->rqueue);
		gboolean pnjs;
		struct restore_job *rj;
		struct datafiles *df;
		struct table_data *tableData=NULL;
		switch (job->type){
			case JOB_DATABASE:
				g_message("Creating Schema");
				rj = (struct restore_job *)(job->job_data);
				df = rj->datafile;
				add_job( conf->queue, rj->database, NULL, df , JOB_DATABASE);
				destroy_job(&job);
				pnjs=push_next_job(conf);
				if (pnjs)
					continue;
				break;
			case JOB_ADD_SCHEMA:
				g_message("Adding Schema");
				rj = (struct restore_job *)(job->job_data);
				df = rj->datafile;
				if (rj->database == NULL){
					rj->database=g_strdup(source_db);
				}
                                tableData=get_table(*ordered_tables,rj->database,rj->table);
                                if (tableData==NULL ){
       					tableData=new_table(rj->database,rj->table);
					conf->ordered_tables = g_slist_append (conf->ordered_tables,tableData);
                                }else{
					g_message("Adding Schema and table found!");
				}
				parsing_create_statement(df->ddl_statement->str, tableData, conf);
				destroy_job(&job);
				pnjs=push_next_job(conf);
				if (pnjs)
					continue;
				break;
			case JOB_ADD_DATA:
//				g_message("Adding Data");
				rj = (struct restore_job *)(job->job_data);
				df = rj->datafile;
				tableData=get_table(*ordered_tables,rj->database,rj->table);
        			if (tableData==NULL ){
			                tableData=new_table(rj->database,rj->table);
			                conf->ordered_tables = g_slist_append (conf->ordered_tables,tableData);
			        }
				tableData->datafiles_list=g_slist_append(tableData->datafiles_list,df);
                                g_free((struct restore_job *)(job->job_data));
                                g_free(job);
				pnjs=push_next_job(conf);
				if (pnjs)
					continue;
				break;
			case JOB_SCHEMA:
				rj = (struct restore_job *)(job->job_data);
				df = rj->datafile;
			        tableData=get_table(*ordered_tables,rj->database,rj->table);
				tableData->status=t_CREATED;
                                df->status=f_TERMINATED;
                                destroy_job(&job);
				break;
			case JOB_INDEX:
				rj = (struct restore_job *)(job->job_data);
				df = rj->datafile;
                                tableData=get_table(*ordered_tables,rj->database,rj->table);
                                tableData->status=t_TERMINATED;
                                df->status=f_TERMINATED;
                                destroy_job(&job);
                                break;
               		case JOB_RESTORE:
                                //tableData=get_table(*ordered_tables,rj->database,rj->table);
				rj = (struct restore_job *)(job->job_data);
				df = rj->datafile;
				df->status=f_TERMINATED;
				destroy_datafile(df);
                                destroy_job(&job);
       		        	break;
			case JOB_SHUTDOWN:
				g_message("JOB_SHUTDOWN");
				destroy_job(&job);
				break;
			case JOB_MESSAGE:
				cc=(char *)(job->job_data);
                		if (  !strcmp(cc,"MYLOADER-ENDITUP")){
		                        return NULL;
		                }
                		if (!strcmp(cc,"MONITOR-ENDITUP")){
		                        canIfinish=TRUE;
				}
				break;
		}

		pnjs=push_next_job(conf); 
		guint amount=0;
		g_slist_foreach(conf->ordered_tables, (GFunc)table_undone, &amount);
//		g_message("Finish: %d \t reader_stopped: %d \t amount: %d PNJS: %d \t RQueued: %d \t QQueued: %d",canIfinish,reader_stopped,amount,pnjs,g_async_queue_length(conf->rqueue),g_async_queue_length(conf->queue));
		if ( !canIfinish && reader_stopped && amount <= num_threads ){
			reader_stopped=FALSE;
			g_message("Feeder UNPaused %d ",amount);
			g_mutex_unlock(db_mutex);
		}
		if (canIfinish && !pnjs && amount == 0  && g_async_queue_length(conf->rqueue) == 0){
	        	struct job *j= g_new0(struct job, 1);
			j->type= JOB_SHUTDOWN;
			g_async_queue_push(conf->queue, j);
		}
		if (canIfinish && !pnjs && g_async_queue_length(conf->rqueue) == 0){
			g_message("Reviewing Status");
			g_slist_foreach(conf->ordered_tables, (GFunc)check_db_status, NULL);
		}
			
		if ( !canIfinish && reader_stopped &&amount > num_threads){
			g_mutex_lock(db_mutex);
			g_message("Feeder Paused %d ",amount);
			reader_stopped=TRUE;
		}
	}
}

void *process_queue(struct thread_data *td) {
	struct configuration *conf= td->conf;
	g_mutex_lock(init_mutex);
	MYSQL *thrconn= mysql_init(NULL);
	g_mutex_unlock(init_mutex);

	mysql_options(thrconn, MYSQL_READ_DEFAULT_GROUP, "myloader");

	if (compress_protocol)
		mysql_options(thrconn, MYSQL_OPT_COMPRESS, NULL);

	if (!mysql_real_connect(thrconn, hostname, username, password, NULL, port, socket_path, 0)) {
		g_critical("Failed to connect to MySQL server: %s", mysql_error(thrconn));
		exit(EXIT_FAILURE);
	}

	if (mysql_query(thrconn, "SET SESSION wait_timeout = 2147483")){
		g_warning("Failed to increase wait_timeout: %s", mysql_error(thrconn));
	}

	if (!enable_binlog)
		mysql_query(thrconn, "SET SQL_LOG_BIN=0");

	mysql_query(thrconn, "/*!40101 SET NAMES binary*/");
	mysql_query(thrconn, "/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */");
	mysql_query(thrconn, "/*!40014 SET UNIQUE_CHECKS=0 */");
	mysql_query(thrconn, "SET autocommit=0");

	g_async_queue_push(conf->ready, GINT_TO_POINTER(1));

	struct job* job= NULL;
	struct restore_job* rj= NULL;
	for(;;) {
		job= (struct job*)g_async_queue_pop(conf->queue);
		struct datafiles* df=NULL;
		switch (job->type) {
			case JOB_RESTORE:
				rj= (struct restore_job *)job->job_data;
				df =rj->datafile;
				if (df->filename==NULL){
					g_message("Thread %d restoring `%s`.`%s` statement", td->thread_id, rj->database, rj->table);
					restore_string(thrconn, rj->database, rj->table, df->dml_statement, FALSE) ;	
				}else{
					g_message("Thread %d restoring `%s`.`%s` filename %s part %d", td->thread_id, rj->database, rj->table, df->filename, df->part);
					restore_data(thrconn, rj->database, rj->table, df->filename, FALSE, TRUE);
				}
				g_async_queue_push(conf->rqueue, job);
				break;
			case JOB_DATABASE:
				rj= (struct restore_job *)job->job_data;
				df =rj->datafile;
				g_message("Thread %d restoring database on `%s`", td->thread_id, rj->database);
				add_schema_string(rj->database, NULL, df->ddl_statement, thrconn);
				g_async_queue_push(conf->rqueue, job);
				break;
			case JOB_SCHEMA:
				rj= (struct restore_job *)job->job_data;
				df =rj->datafile;
				g_message("Thread %d restoring schema on `%s`.`%s`", td->thread_id, rj->database, rj->table);
				add_schema_string(rj->database, rj->table, df->ddl_statement, thrconn);
				g_async_queue_push(conf->rqueue, job);
				break;
			case JOB_INDEX:
                                rj= (struct restore_job *)job->job_data;
                                df =rj->datafile;
                                g_message("Thread %d restoring indexes on `%s`.`%s`", td->thread_id, rj->database, rj->table);
                                restore_string(thrconn, rj->database, rj->table, df->ddl_statement, FALSE);
                                g_async_queue_push(conf->rqueue, job);
                                break;
			case JOB_SHUTDOWN:
				g_message("Thread %d shutting down", td->thread_id);
				if (thrconn)
					mysql_close(thrconn);
				mysql_thread_end();
				g_async_queue_push(conf->rqueue, job);
				return NULL;
				break;
			default:
				g_critical("Something very bad happened!");
				exit(EXIT_FAILURE);
		}
	}
	if (thrconn)
		mysql_close(thrconn);
	mysql_thread_end();
	return NULL;
}

void restore_data(MYSQL *conn, char *database, char *table, const char *filename, gboolean is_schema, gboolean need_use) {
	void *infile;
	gboolean is_compressed= FALSE;
	gboolean eof= FALSE;
	guint query_counter= 0;
	GString *data= g_string_sized_new(1024);

	gchar* path= g_build_filename(directory, filename, NULL);

	if (!g_str_has_suffix(path, ".gz")) {
		infile= g_fopen(path, "r");
		is_compressed= FALSE;
	} else {
		infile= (void*) gzopen(path, "r");
		is_compressed= TRUE;
	}

	if (!infile) {
		g_critical("cannot open file %s (%d)", filename, errno);
		errors++;
		return;
	}


	if(need_use){
		gchar *query= g_strdup_printf("USE `%s`", db ? db : database);

		if (mysql_query(conn, query)) {
			g_critical("Error switching to database %s whilst restoring table %s", db ? db : database, table);
			g_free(query);
			errors++;
			return;
		}

		g_free(query);
	}

	if ( ! dry_run){	
	if (!is_schema)
		mysql_query(conn, "START TRANSACTION");

	while (eof == FALSE) {
		if (read_data(infile, is_compressed, data, &eof)) {
			// Search for ; in last 5 chars of line
			if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) { 
				if (mysql_real_query(conn, data->str, data->len)) {
					g_critical("Error restoring %s.%s from file %s: %s", db ? db : database, table, filename, mysql_error(conn));
					errors++;
					return;
				}
				query_counter++;
				if (!is_schema &&(query_counter == commit_count)) {
					query_counter= 0;
					if (mysql_query(conn, "COMMIT")) {
						g_critical("Error committing data for %s.%s: %s", db ? db : database, table, mysql_error(conn));
						errors++;
						return;
					}
					mysql_query(conn, "START TRANSACTION");
				}

				g_string_set_size(data, 0);
			}
		} else {
			g_critical("error reading file %s (%d)", filename, errno);
			errors++;
			return;
		}
	}
	if (!is_schema && mysql_query(conn, "COMMIT")) {
		g_critical("Error committing data for %s.%s from file %s: %s", db ? db : database, table, filename, mysql_error(conn));
		errors++;
	}
	}
	g_string_free(data, TRUE);
	g_free(path);
	if (!is_compressed) {
		fclose(infile);
	} else {
		gzclose((gzFile)infile);
	}	
	return;
}

int restore_string(MYSQL *conn, char *database, char *table, GString *statement, gboolean is_schema) {
        guint query_counter= 0;
        int i;
        gchar** split_file= g_strsplit(statement->str, "\n\n", -1);
        gchar *query= g_strdup_printf("USE `%s`", db ? db : database);

        if (! dry_run){
        	if (mysql_query(conn, query)) {
	                g_critical("Error switching to database %s whilst restoring table %s", db ? db : database, table);
                	g_free(query);
        	        errors++;
			g_strfreev(split_file);
                	return 1;
        	}
	        if (mysql_query(conn, query)) {
                	g_critical("Error switching to database %s whilst restoring table %s", db ? db : database, table);
        	        g_free(query);
	                errors++;
			g_strfreev(split_file);
        	        return 1;
	        }
        	for (i=0; i < (int)g_strv_length(split_file);i++){
	                GString *data=g_string_new(split_file[i]);

                	if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) {

        	                if (mysql_real_query(conn, data->str, data->len)) {
	                                g_critical("Error restoring %s.%s from string statement: %s", db ? db : database, table, mysql_error(conn));
                                	g_critical("Error restoring String: %s", data->str);
                        	        errors++;
					g_strfreev(split_file);
					g_string_free(data,TRUE);
					g_free(query);
	                                return 1;
                        	}
                	        query_counter++;
        	                if ((!is_schema ) && ((query_counter == commit_count) || (data->str[data->len-2] == ';' ))) {
	                                query_counter= 0;
                                	if (mysql_query(conn, "COMMIT")) {
                        	                g_critical("Error committing data for %s.%s: %s", db ? db : database, table, mysql_error(conn));
                	                        errors++;
						g_strfreev(split_file);
						g_string_free(data,TRUE);
						g_free(query);
                        	                return 1;
                	                }
        	                        mysql_query(conn, "START TRANSACTION");
	                        }
                	}
			g_string_free(data,TRUE);
	        }
        }
	g_strfreev(split_file);
	g_free(query);
        return 0;
}

gboolean read_data(FILE *file, gboolean is_compressed, GString *data, gboolean *eof) {
	char buffer[512];

	do {
		if (!is_compressed) {
			if (fgets(buffer, 512, file) == NULL) {
				if (feof(file)) {
					*eof= TRUE;
					buffer[0]= '\0';
				} else {
					return FALSE;
				}
			}
		} else {
			if (!gzgets((gzFile)file, buffer, 512)) {
				if (gzeof((gzFile)file)) {
					*eof= TRUE;
					buffer[0]= '\0';
				} else {
					return FALSE;
				}
			}
		}
		g_string_append(data, buffer);
	} while ((buffer[strlen(buffer)] != '\0') && *eof == FALSE);

	return TRUE;
}

gboolean read_line(FILE *file, gboolean is_compressed, GString *data, gboolean *eof) {
	const int buffersize=512;
        char buffer[buffersize];
        do {
                if (!is_compressed) {
                        if (fgets(buffer, buffersize, file) == NULL) {
                                if (feof(file)) {
                                        *eof= TRUE;
                                        buffer[0]= '\0';
                                } else {
                                        return FALSE;
                                }
                        }
                } else {
                        if (!gzgets((gzFile)file, buffer, buffersize)) {
                                if (gzeof((gzFile)file)) {
                                        *eof= TRUE;
                                        buffer[0]= '\0';
                                } else {
                                        return FALSE;
                                }
                        }
                }
		if (buffer[0] != '\n' && strlen(buffer)>0)
                	g_string_append(data, buffer);
        } while ( strlen(buffer)>1 && (buffer[strlen(buffer)-1] != '\n') && *eof == FALSE);

        return TRUE;
}

void read_file_process( struct configuration *conf){
	GAsyncQueue* lqueue=conf->squeue;
	FILE *infile;
	gboolean eof=FALSE;
	infile= g_fopen(inputfile, "r");
	GString *statement=g_string_new("");
	g_message("Starting read file process");
	while (!feof(infile) && !eof){
		GString *data=g_string_new("");
		read_line(infile,FALSE,data,&eof);
		if (data != NULL && data->str != NULL && strlen(data->str) > 2){
			g_string_append(statement,data->str);
			int m=strlen(data->str)-2;
			if (data->str[m]== ';'){
				g_async_queue_push(lqueue,statement);
				statement=g_string_new("");
			}
		}
		g_string_free(data,TRUE);
		g_mutex_lock(db_mutex);
		g_mutex_unlock(db_mutex);	
	}
	statement=g_string_new("DBFEEDER-ENDITUP");
        g_async_queue_push(lqueue, statement);
	g_message("End of File");
}


void db_feeder(struct configuration *conf){
        GAsyncQueue* lqueue=conf->squeue;
	GString *finalstatement=g_string_new("");
	g_message("Starting Feeder");
	char *databaseusage=source_db;
	conf->ordered_tables=NULL; 
	// Hay que leer los statements uno por uno y ver de enviarlos a la DB
	for(;;){	
		GString *statement=g_async_queue_pop(lqueue);
		g_string_append(finalstatement,statement->str);
		char *database=NULL;
                char *table=NULL;
		if (!g_ascii_strncasecmp("INSERT ",statement->str,7)){
			gchar** split_dbname_tablename= g_strsplit(statement->str, " ", 6);
			database=databaseusage;
			if (split_dbname_tablename!=NULL && split_dbname_tablename[0]!=NULL && split_dbname_tablename[2]!=NULL){
				read_database_table(split_dbname_tablename[2],&database,&table);	
/*				gchar** split_db_table= g_strsplit(split_dbname_tablename[2],"`", 3);
				int i=0;
				char* last=NULL;
				while (split_db_table[i]!=NULL){
					if (strlen(split_db_table[i])>0){
						if (split_db_table[i][0]=='.'){
							database=last;
						}
						last=split_db_table[i];
					}
					i++;
				}
				g_strfreev(split_db_table);
				table=last;
			*/
			}
			g_strfreev(split_dbname_tablename);
			add_job(conf->rqueue,database,table,new_datafile_dml_statement(g_string_new(finalstatement->str)),JOB_ADD_DATA);
			g_string_free(finalstatement,TRUE);
			finalstatement=g_string_new("");
		}else{
		if (!g_ascii_strncasecmp("USE ",statement->str,4)){
			statement->str[strlen(statement->str)-2]='\0';
			databaseusage=g_strdup_printf("%s",&(statement->str[4]));
                        g_message("USE %s",databaseusage);
			g_string_free(finalstatement,TRUE);
                        finalstatement=g_string_new("");
                }else{
		if (!g_ascii_strncasecmp("CREATE TABLE ",statement->str,13)){
                        gchar** split_dbname_tablename= g_strsplit(statement->str, " ", 6);
                        database=databaseusage;
                        if (split_dbname_tablename!=NULL && split_dbname_tablename[0]!=NULL && split_dbname_tablename[2]!=NULL)
				read_database_table(split_dbname_tablename[2],&database,&table);
			g_strfreev(split_dbname_tablename);
			add_job(conf->rqueue,database,table,new_datafile_ddl_statement(g_string_new(finalstatement->str)),JOB_ADD_SCHEMA);
			g_string_free(finalstatement,TRUE);
			finalstatement=g_string_new("");
                }else{
		if (!g_ascii_strncasecmp("CREATE DATABASE ",statement->str,16)){
                        gchar** split_dbname_tablename= g_strsplit(statement->str, " ", 6);
                        database=databaseusage;
                        if (split_dbname_tablename!=NULL && split_dbname_tablename[0]!=NULL && split_dbname_tablename[2]!=NULL)
				read_database_table(split_dbname_tablename[2],&database,&table);
			g_strfreev(split_dbname_tablename);
			add_job(conf->rqueue,database,table,new_datafile_ddl_statement(g_string_new(finalstatement->str)),JOB_DATABASE);
			g_string_free(finalstatement,TRUE);
			finalstatement=g_string_new("");
                }else{
			g_string_append(finalstatement,statement->str);
			g_string_append(finalstatement,"\n");
		}
		}
		}
		}
                if (!strcmp(statement->str,"DBFEEDER-ENDITUP")){
			add_message_job(conf->rqueue,"MONITOR-ENDITUP");
			g_message("Stopping Feeder");
                        return;
		}
	}

}


void add_message_job( GAsyncQueue* queue, const char * message) {
        struct job *j= g_new0(struct job, 1);

        j->job_data= (void*) message;
        j->type = JOB_MESSAGE;

        g_async_queue_push(queue, j);
        return;
}

void add_job( GAsyncQueue* queue, char * database, char * table, struct datafiles *df , enum job_type jt) {
        struct job *j= g_new0(struct job, 1);

        struct restore_job *rj= g_new(struct restore_job, 1);

        j->job_data= (void*) rj;
	j->type = jt;
	
        rj->database= g_strdup(database);
        rj->table= g_strdup(table);
        rj->datafile=df;

        g_async_queue_push(queue, j);
        return;
}


void read_database_table (char * split_dbname_tablename, char **database, char **table) {
	gchar** split_db_table= g_strsplit(split_dbname_tablename,"`", -1);
        int i=0;
        char* last=NULL;
        while (split_db_table[i]!=NULL){
        	if (strlen(split_db_table[i])>0){
                	if (split_db_table[i][0]=='.'){
                        	*database=strdup(last);
                        }
                        last=split_db_table[i];
                }
                i++;
        }
	*table=strdup(last);
	g_strfreev(split_db_table);
}




void parsing_create_statement(char *data, struct table_data *td, struct configuration *conf){
        // Will read line by line
        
	gchar** split_file= g_strsplit(data, "\n", -1);
	//if (td->schema != NULL)
		td->schema=new_datafile_ddl_statement(g_string_new(""));
				char *table    = td->table;
				char *database = td->database;
                                int i,commafix=0;

                                // Parsing the lines
                                for (i=0; i < (int)g_strv_length(split_file);i++){
                                        if (   g_strrstr(split_file[i],"  KEY")
                                                        || g_strrstr(split_file[i],"  UNIQUE")
                                                        || g_strrstr(split_file[i],"  SPATIAL")
                                                        || g_strrstr(split_file[i],"  FULLTEXT")
                                                        || g_strrstr(split_file[i],"  INDEX")
                                                ){
                                                // This line is a index
                                                commafix=1;
                                                gchar *poschar=g_strrstr(split_file[i],",");
                                                if (poschar && *(poschar+1)=='\0')
                                                        *poschar='\0';

                                                if (td->indexes == NULL){
                                                        td->indexes=new_datafile_ddl_statement(g_string_new ("/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n\nALTER TABLE `"));
                                                        GString *ni = td->indexes->ddl_statement;
                                                        ni=g_string_append(ni, table);
                                                        ni=g_string_append(ni, "` ");
                                                }else{
                                                        GString *ni = td->indexes->ddl_statement;
                                                        ni=g_string_append(ni, ",");
                                                }
                                                GString *ni = td->indexes->ddl_statement;
                                                ni= g_string_append(g_string_append(ni,"\n  ADD "), split_file[i]);
                                        }else
                                        if ( g_strrstr(split_file[i],"  CONSTRAINT") ){
                                                // This line is a constraint
                                                commafix=1;
                                                gchar *poschar=g_strrstr(split_file[i],",");
                                                if (poschar && *(poschar+1)=='\0')
                                                        *poschar='\0';
                                                if (td->constraints == NULL ){
                                                        td->constraints=new_datafile_ddl_statement(g_string_new ("/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n\nALTER TABLE `"));
                                                        GString *ni = td->constraints->ddl_statement;
                                                        ni=g_string_append(ni, table);
                                                        ni=g_string_append(ni, "` ");
                                                }else{
                                                        GString *ni = td->constraints->ddl_statement;
                                                        ni=g_string_append(ni, ",");
                                                }
                                                GString *ni = td->constraints->ddl_statement;
                                                ni=g_string_append(g_string_append(ni,"\n  ADD "), split_file[i]);
                                        }else{
                                                // This line is a piece of the create table statement
                                                if (commafix){
                                                        commafix=0;
                                                        gchar *poschar=g_strrstr(td->schema->ddl_statement->str,",");
                                                        if (poschar && *(poschar+1)=='\0')
                                                                *poschar=' ';
                                                }
                                                GString *ni = td->schema->ddl_statement;
                                                ni=g_string_append(g_string_append(ni,"\n"), split_file[i]);
                                        }
                                }

                                // Set the constraints to the table
                                if (td->constraints != NULL){
                                        g_message("Spliting constraint for `%s`.`%s`", db ? db : database, table);
                                        td->constraints->ddl_statement=g_string_append(td->constraints->ddl_statement,";\n");
                                        conf->constraint_list = g_slist_append (conf->constraint_list,td);
                                }

                                // Set the indexes to the table
                                if (td->indexes != NULL){
                                        td->indexes->ddl_statement=g_string_append(td->indexes->ddl_statement,";\n");
                                }
	g_strfreev(split_file);
}





