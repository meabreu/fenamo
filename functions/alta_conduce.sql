-- Function: fenamosq.alta_conduce(numeric, numeric, character varying, character varying, timestamp without time zone, numeric, character varying)

-- DROP FUNCTION fenamosq.alta_conduce(numeric, numeric, character varying, character varying, timestamp without time zone, numeric, character varying);

CREATE OR REPLACE FUNCTION fenamosq.alta_conduce(
    IN pi_id_order numeric,
    IN pi_quantity numeric,
    IN pi_description character varying,
    IN pi_master character varying,
    IN pi_tran_date timestamp without time zone,
    IN pi_tran_id numeric,
    IN pi_tran_user character varying,
    OUT po_id_conduce numeric,
    OUT po_tran_id numeric,
    OUT po_cod_err character varying,
    OUT po_msj_err character varying,
    OUT po_msj_err_pl character varying)
  RETURNS record AS
$BODY$
  DECLARE
      c_cod_transaction  varchar(200):='ALTA_ORD_ORDER';
      v_tran_id          NUMERIC;
      v_tran_user        VARCHAR(50);
      v_tran_date        TIMESTAMP;
      c_falso            varchar(2):='0';  
      v_aplica           varchar(2):='1';
      c_aplica           varchar(2):='1';
      v_id_task          numeric;
      task_record        record;
      task_js            json;
      v_descrip          varchar(200);
      v_id_order   numeric;
  v_quant_dipatched  numeric;
  v_quant_order      numeric;
  BEGIN 
       
      --Obtener valores transacccionales
      IF pi_master = c_aplica THEN 
          
          SELECT rs.po_tran_id,
                 rs.po_tran_date,
                 rs.po_cod_err,
                 rs.po_msj_err,
                 rs.po_msj_err_pl
           INTO  v_tran_id,
                 v_tran_date,
                 po_cod_err,
                 po_msj_err,
                 po_msj_err_pl
           FROM fenamosq.get_transaction(
                pi_cod_transaction    => c_cod_transaction,
                pi_tran_user          => pi_tran_user
           ) rs;

          IF po_cod_err <> 'OK' THEN 
              RAISE EXCEPTION 'TRANSFER';
          END IF;

      ELSIF pi_tran_date IS NULL OR  pi_tran_id IS NULL  OR pi_tran_user  IS NULL THEN
          po_cod_err := 'ALTA-ORDER-001';
          RAISE EXCEPTION 'CONDICTION';
      ELSE 
          v_tran_id := pi_tran_id;
          v_tran_date  := pi_tran_date;
      END IF; 

      SELECT rs.po_aplica 
        INTO v_aplica 
        FROM fenamosq.validate_transaction(
              pi_cod_transaction => c_cod_transaction,
              pi_tran_user        => pi_tran_user
            ) rs;

      IF v_aplica = '0' THEN 
          po_cod_err := 'ALTA-ORDER-002';
          RAISE EXCEPTION 'CONDICTION';
      ELSIF v_aplica = '2' THEN 
          po_cod_err := 'ALTA-ORDER-003';
          RAISE EXCEPTION 'CONDICTION';
      END IF;

      po_cod_err := 'ALTA-ORDER-004';
      
      INSERT INTO fenamosq.ord_conduce(
            id_order, date_created, description, quant_dispatched, state, tran_date, tran_id, tran_user)
      VALUES (pi_id_order, now(), pi_description, pi_quantity,'A',v_tran_date, v_tran_id, pi_tran_user)
        returning id_conduce into po_id_conduce;

po_cod_err := 'ALTA-ORDER-005';
      UPDATE fenamosq.ord_order
         SET quant_dispatched = quant_dispatched + pi_quantity
       WHERE id_order = pi_id_order;

po_cod_err := 'ALTA-ORDER-006';
      SELECT quant_dispatched, quant_order
        INTO v_quant_dipatched, v_quant_order
        FROM fenamosq.ord_order
       WHERE id_order = pi_id_order;

po_cod_err := 'ALTA-ORDER-007';
      IF v_quant_dipatched = v_quant_order THEN
        UPDATE fenamosq.ord_order 
           SET order_state = 'DISPATCHED'
         WHERE id_order = pi_id_order;

po_cod_err := 'ALTA-ORDER-008';
       INSERT INTO fenamosq.ord_order_hist(
            id_order, ord_orden_seq, id_client, date_created, cod_priority, description, 
          observation, order_state, quant_order, state, tran_date, tran_id, tran_user,quant_finished, quant_dispatched)
       SELECT id_order, id_order, id_client, now(), cod_priority, description, 
             observation,order_state, quant_order, 'A', v_tran_date, v_tran_id, pi_tran_user,quant_finished,  quant_dispatched
         FROM fenamosq.ord_order 
        WHERE id_order = pi_id_order;

      END IF;
      
      po_cod_err := 'OK';
      po_tran_id := v_tran_id;
  EXCEPTION
   WHEN OTHERS THEN 
        po_tran_id := v_tran_id;

        IF(SQLERRM <> 'TRANSFER') THEN 
            po_msj_err_pl = SQLERRM;
        END IF;

        IF pi_master = c_aplica THEN
          
          SELECT rs.po_msj_err
            into po_msj_err 
            FROM fenamosq.set_transaction_log(
                pi_tran_id         => v_tran_id,
                pi_tran_cod        => c_cod_transaction,
                pi_tran_date       => v_tran_date,    
                pi_tran_user       => pi_tran_user,
                pi_cod_err         => po_cod_err,
                pi_msj_err_pl      => po_msj_err_pl) rs;
        END IF;


  END; 
  $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION fenamosq.alta_conduce(numeric, numeric, character varying, character varying, timestamp without time zone, numeric, character varying)
  OWNER TO fenamo;
