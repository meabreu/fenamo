-- DROP FUNCTION fenamosq.set_user_task(bigint, character varying, character varying, timestamp without time zone, numeric, character varying);

CREATE OR REPLACE FUNCTION fenamosq.set_user_task(
    IN pi_id_order_task bigint,
    IN pi_cod_user character varying,
    IN pi_master character varying,
    IN pi_tran_date timestamp without time zone,
    IN pi_tran_id numeric,
    IN pi_tran_user character varying,
    OUT po_tran_id numeric,
    OUT po_cod_err character varying,
    OUT po_msj_err character varying,
    OUT po_msj_err_pl character varying)
  RETURNS record AS
$BODY$
  DECLARE
      c_cod_transaction  varchar(200):='ALTA_ORDER_TASK';
      v_tran_id      NUMERIC;
      v_tran_user  VARCHAR(50);
      v_tran_date  TIMESTAMP;     
      v_end_date  TIMESTAMP;     
      v_validar  varchar(20);
      c_falso    varchar(20):='0';
      v_aplica    varchar(2):='1';
      c_aplica    varchar(2):='1';
      v_id_order_task  numeric;
      v_id_order_task_hist  numeric;
      v_rec_ord_order_task  RECORD;
      v_rec_ord_order       RECORD;
      v_ord_state           VARCHAR(20);
      v_completed     NUMERIC;
      v_task_state    varchar(20);
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
          po_cod_err := 'MOD-ORDER-TASK-001';
          RAISE EXCEPTION 'EXCEPTION';
      ELSE 
          v_tran_id := pi_tran_id;
          v_tran_date  := pi_tran_date;
      END IF; 

      SELECT rs.po_aplica 
        INTO v_aplica 
        FROM fenamosq.validate_transaction(
              pi_cod_transaction => c_cod_transaction,
              pi_tran_user        => v_tran_user
            ) rs;

      IF v_aplica = '0' THEN 
          po_cod_err := 'MOD-ORDER-TASK-002';
          RAISE EXCEPTION 'CONDICTION';
      ELSIF v_aplica = '2' THEN 
          po_cod_err := 'MOD-ORDER-TASK-003';
          RAISE EXCEPTION 'CONDICTION';
      END IF;

      po_cod_err := 'MOD-ORDER-TASK-004';
  
      SELECT *
        INTO v_rec_ord_order_task
        FROM fenamosq.ord_order_task
       WHERE id_order_task = pi_id_order_task;


      SELECT *
        INTO v_rec_ord_order
        FROM fenamosq.ord_order
       WHERE id_order = v_rec_ord_order_task.id_order;

      po_cod_err := 'MOD-ORDER-TASK-005';
      UPDATE fenamosq.ord_order_task
         SET task_state = 'ASIGNED',
             user_asign = pi_cod_user,
             tran_user = pi_tran_user,
             tran_date = v_tran_date,
             tran_id = v_tran_id

       WHERE id_order_task = pi_id_order_task;

      po_cod_err := 'MOD-ORDER-TASK-006';
      
       INSERT INTO fenamosq.ord_order_task_hist
       (id_order_task, id_order, id_task, quant_order, date_created, date_started, 
              date_finished, description, observation, task_state, time_waited, 
              time_worked, time_paused, time_total, state, tran_date, tran_id, tran_user,quant_completed,quant_worked,user_asign)
         SELECT id_order_task, id_order, id_task, quant_order, date_created, date_started, 
              date_finished, description, observation, task_state, time_waited, 
              time_worked, time_paused, time_total, state, tran_date, tran_id, tran_user,quant_completed,quant_worked,pi_cod_user
          FROM fenamosq.ord_order_task
         WHERE id_order_task = pi_id_order_task;

        SELECT MIN(quant_completed)
          INTO v_completed
          FROM fenamosq.ord_order_task
         WHERE id_order = v_rec_ord_order.id_order;

         IF v_completed = v_rec_ord_order.quant_order THEN
            v_ord_state := 'COMPLETED';
         ELSE
            v_ord_state = 'IN_PROCCESS';
         END IF;

         IF v_completed > v_rec_ord_order.quant_finished THEN
            UPDATE fenamosq.ord_order 
               SET quant_finished = v_completed,
                   order_state = v_ord_state
             WHERE id_order = v_rec_ord_order.id_order;

           INSERT INTO fenamosq.ord_order_hist(
                id_order, ord_orden_seq, id_client, date_created, cod_priority, description, 
              observation, order_state, quant_order, state, tran_date, tran_id, tran_user,quant_finished, quant_dispatched,user_asign)
           SELECT id_order, id_order, id_client, now(), cod_priority, description, 
                 observation,order_state, quant_order, 'A', v_tran_date, v_tran_id, pi_tran_user,quant_finished,  quant_dispatched,pi_cod_user
             FROM fenamosq.ord_order 
            WHERE id_order = v_rec_ord_order.id_order;

         ELSIF v_rec_ord_order.order_state <> v_ord_state THEN 
          UPDATE fenamosq.ord_order 
               SET quant_finished = v_completed,
                   order_state = v_ord_state
             WHERE id_order = v_rec_ord_order.id_order;

           INSERT INTO fenamosq.ord_order_hist(
                id_order, ord_orden_seq, id_client, date_created, cod_priority, description, 
              observation, order_state, quant_order, state, tran_date, tran_id, tran_user,quant_finished, quant_dispatched,user_asign)
           SELECT id_order, id_order, id_client, now(), cod_priority, description, 
                 observation,order_state, quant_order, 'A', v_tran_date, v_tran_id, pi_tran_user,quant_finished,  quant_dispatched,pi_cod_user
             FROM fenamosq.ord_order 
            WHERE id_order = v_rec_ord_order.id_order;
          
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
ALTER FUNCTION fenamosq.set_user_task(bigint, character varying, character varying, timestamp without time zone, numeric, character varying)
  OWNER TO fenamo;
