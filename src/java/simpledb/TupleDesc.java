package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    private ArrayList<TDItem> tdItems; 

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int lenType = typeAr.length;
        int lenField = fieldAr.length;

        if(lenField > lenType || lenType < 1){
            /*invalid parameters, do something*/
        }
        tdItems = new ArrayList<TDItem>();
        int i;
        for(i=0; i<lenField; i++){
            tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
        if(i<lenType){ 
            //null name fields exist
            for(int j=i; j<lenType; j++){
                tdItems.add(new TDItem(typeAr[j], ""));
            }
        } 
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        int len = typeAr.length;
        if(len<1){
            /*invalid input, do something*/
        }
        tdItems = new ArrayList<TDItem>();
        for(int i=0; i<len; i++){
            tdItems.add(new TDItem(typeAr[i], ""));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if(i<0 || i>=this.numFields()) throw new NoSuchElementException();
        return this.tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if(i<0 || i>=this.numFields()) throw new NoSuchElementException();
        return this.tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	int index = -1;
        for(int i=0; i<this.numFields(); i++){
            if(this.getFieldName(i).equals(name)) index = i;
        }
        if(index==-1) throw new NoSuchElementException();
        return index;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for(int i=0; i<this.numFields(); i++){
            size += this.getFieldType(i).getLen();
        }
        return size;
    }

    /**
     * Return the TDItems List of the TupleDesc.
     * @return tuple schema list
     */
    public ArrayList<TDItem> getItemsList(){
        return this.tdItems;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        ArrayList<TDItem> list1 = td1.getItemsList();
        ArrayList<TDItem> list2 = td2.getItemsList();
        int len1 = td1.numFields(), len2 = td2.numFields();
        Type[] type = new Type[len1+len2]; 
        String[] name = new String[len1+len2];
        int k = 0;
        for(int i=0; i<len1; i++){
            type[k] = list1.get(i).fieldType;
            name[k] = list1.get(i).fieldName;
            k++;
        }
        for(int j=0; j<len2; j++){
            type[k] = list2.get(j).fieldType;
            name[k] = list2.get(j).fieldName;
            k++;
        }
        return new TupleDesc(type, name);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if(o instanceof TupleDesc){
            if(((TupleDesc) o).getSize()!=this.getSize()) return false;
            else{
                for(int i=0; i<this.numFields(); i++){
                    if(((TupleDesc) o).getFieldType(i)!=this.getFieldType(i))
                        return false;
                }
            }
            return true;
        }else return false;
        
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for(TDItem item : tdItems){
            sb.append(item.fieldType + "(" + item.fieldName + "),");
        }
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }
}
