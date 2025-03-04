from fastapi import APIRouter, HTTPException, Query, Body
from sqlalchemy import inspect, text
from datetime import datetime
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker
from typing import Any, Dict, Optional

# Model for soft delete operations
class SoftDeletePayload(BaseModel):
    deleted_by_guid: int

class ApiBridgeRouter:
    """
    Dynamic CRUD router for FastAPI that can be used with any database table.
    """
    
    def __init__(self, engine, prefix="/base"):
        """
        Initialize the dynamic CRUD router.
        
        Args:
            engine: SQLAlchemy engine
            prefix (str): API route prefix
        """
        self.engine = engine
        self.prefix = prefix
        self.router = APIRouter(prefix=prefix)
        self.session_factory = sessionmaker(bind=engine)
        
        # Register routes
        self._register_routes()
    
    def get_router(self):
        """Return the configured router"""
        return self.router
    
    def get_table_columns(self, table_name: str):
        """
        Fetch the columns for a given table from the database.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            dict: Column names and types
        """
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name)
        return {column['name']: column['type'] for column in columns}

    def _register_routes(self):
        """Register all CRUD routes"""
        
        @self.router.get("/test")
        def test_db_connection():
            """Test the database connection"""
            try:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return {"message": "Database connection successful"}
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get("/{table_name}")
        def get_all_records(table_name: str, page: int = Query(1, ge=1), limit: int = Query(10, ge=1)):
            """
            Fetch all records from the given table with pagination.
            
            Args:
                table_name (str): Name of the table
                page (int): Page number (starts at 1)
                limit (int): Number of records per page
                
            Returns:
                dict: Records and pagination metadata
            """
            session = self.session_factory()

            # Calculate the offset based on the page number and limit
            offset = (page - 1) * limit
            
            try:
                # Query to get the records with pagination
                query = text(f"SELECT * FROM {table_name} LIMIT :limit OFFSET :offset")
                result = session.execute(query, {"limit": limit, "offset": offset}).fetchall()

                # Query to get the total number of records for pagination metadata
                count_query = text(f"SELECT COUNT(*) FROM {table_name}")
                total_records = session.execute(count_query).scalar()

                # Convert result to list of dictionaries
                columns = [column['name'] for column in inspect(self.engine).get_columns(table_name)]
                result_dict = [dict(zip(columns, row)) for row in result]

                # Prepare pagination metadata
                pagination = {
                    "total_records": total_records,
                    "limit": limit,
                    "skip": offset,
                    "total_pages": (total_records // limit) + (1 if total_records % limit else 0),
                    "current_page": page,
                }

                return {
                    "data": result_dict,
                    "pagination": pagination
                }

            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error reading table: {str(e)}")
            finally:
                session.close()

        @self.router.post("/{table_name}")
        def create_record(table_name: str, record: Dict[str, Any] = Body(...)):
            """
            Insert a new record into the table.
            
            Args:
                table_name (str): Name of the table
                record (dict): Record data
                
            Returns:
                dict: Success message
            """
            session = self.session_factory()
            try:
                # Create placeholders for the values
                columns = ", ".join(record.keys())
                placeholders = ", ".join([f":{key}" for key in record.keys()])
                
                # Create the query using text()
                query = text(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})")
                
                # Execute with parameters
                session.execute(query, record)
                session.commit()
                
                return {"message": f"Record added to {table_name}"}
            
            except Exception as e:
                session.rollback()
                raise HTTPException(status_code=500, detail=f"Error inserting record: {str(e)}")
            
            finally:
                session.close()

        @self.router.put("/{table_name}/{record_id}")
        def update_record(table_name: str, record_id: int, record: Dict[str, Any] = Body(...)):
            """
            Update an existing record in the table.
            
            Args:
                table_name (str): Name of the table
                record_id (int): ID of the record to update
                record (dict): Updated record data
                
            Returns:
                dict: Success message
            """
            session = self.session_factory()
            try:
                # Create placeholders for the SET clause
                set_clause = ", ".join([f"{key}=:{key}" for key in record.keys()])
                
                # Create the query using text()
                query = text(f"UPDATE {table_name} SET {set_clause} WHERE id = :record_id")
                
                # Add record_id to the parameters
                params = {**record, "record_id": record_id}
                
                # Execute with parameters
                result = session.execute(query, params)
                session.commit()
                
                if result.rowcount == 0:
                    raise HTTPException(status_code=404, detail=f"Record {record_id} not found in {table_name}")
                
                return {"message": f"Record {record_id} updated in {table_name}"}
            
            except HTTPException as e:
                session.rollback()
                raise e
            except Exception as e:
                session.rollback()
                raise HTTPException(status_code=500, detail=f"Error updating record: {str(e)}")
            
            finally:
                session.close()

        @self.router.delete("/{table_name}/{record_id}")
        def soft_delete_record(
            table_name: str, 
            record_id: int,
            payload: SoftDeletePayload = Body(...)
        ):
            """
            Soft delete a record by updating active, deleted flags and deletion metadata.
            
            Args:
                table_name (str): Name of the table
                record_id (int): ID of the record to soft delete
                payload (SoftDeletePayload): Deletion metadata
                
            Returns:
                dict: Success message and deletion metadata
            """
            session = self.session_factory()
            try:
                current_time = int(datetime.now().timestamp())
                
                query = text(f"""
                    UPDATE {table_name} 
                    SET active = 0, 
                        deleted = 1, 
                        deleted_by_guid = :deleted_by_guid,
                        deleted_at = :deleted_at
                    WHERE id = :record_id
                """)
                
                params = {
                    "record_id": record_id,
                    "deleted_by_guid": payload.deleted_by_guid,
                    "deleted_at": current_time
                }
                
                result = session.execute(query, params)
                session.commit()
                
                if result.rowcount == 0:
                    raise HTTPException(
                        status_code=404, 
                        detail=f"Record {record_id} not found in {table_name}"
                    )
                    
                return {
                    "message": f"Record {record_id} soft deleted from {table_name}",
                    "deleted_at": current_time,
                    "deleted_by": payload.deleted_by_guid
                }
            
            except HTTPException as e:
                session.rollback()
                raise e
            except Exception as e:
                session.rollback()
                raise HTTPException(status_code=500, detail=f"Error soft deleting record: {str(e)}")
            
            finally:
                session.close()

        @self.router.delete("/{table_name}/{record_id}/hard")
        def delete_record(table_name: str, record_id: int):
            """
            Delete a record from the table by ID.
            
            Args:
                table_name (str): Name of the table
                record_id (int): ID of the record to delete
                
            Returns:
                dict: Success message
            """
            session = self.session_factory()
            try:
                # Create the query using text()
                query = text(f"DELETE FROM {table_name} WHERE id = :record_id")
                
                # Execute with parameters
                result = session.execute(query, {"record_id": record_id})
                session.commit()
                
                if result.rowcount == 0:
                    raise HTTPException(
                        status_code=404, 
                        detail=f"Record {record_id} not found in {table_name}"
                    )
                    
                return {"message": f"Record {record_id} deleted from {table_name}"}
            
            except HTTPException as e:
                session.rollback()
                raise e
            except Exception as e:
                session.rollback()
                raise HTTPException(status_code=500, detail=f"Error deleting record: {str(e)}")
            
            finally:
                session.close()

        @self.router.patch("/{table_name}/{record_id}")
        def patch_record(table_name: str, record_id: int, record: Dict[str, Any] = Body(...)):
            """
            Update specific fields of a record.
            
            Args:
                table_name (str): Name of the table
                record_id (int): ID of the record to patch
                record (dict): Partial record data to update
                
            Returns:
                dict: Success message
            """
            session = self.session_factory()
            try:
                # Create placeholders for the SET clause
                set_clause = ", ".join([f"{key}=:{key}" for key in record.keys()])
                
                # Create the query using text()
                query = text(f"UPDATE {table_name} SET {set_clause} WHERE id = :record_id")
                
                # Add record_id to the parameters
                params = {**record, "record_id": record_id}
                
                result = session.execute(query, params)
                session.commit()

                if result.rowcount == 0:
                    raise HTTPException(
                        status_code=404, 
                        detail=f"Record {record_id} not found in {table_name}"
                    )
                    
                return {"message": f"Record {record_id} patched in {table_name}"}
            
            except HTTPException as e:
                session.rollback()
                raise e
            except Exception as e:
                session.rollback()
                raise HTTPException(status_code=500, detail=f"Error patching record: {str(e)}")
            
            finally:
                session.close()